/*
 * Copyright (C) 2017-2026 Universidade da Coruña
 * 
 * This file is part of MarDRe.
 * 
 * MarDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MarDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class SequenceTest {

	private static final short PREFIX_LENGTH = 5;
	private static final SeqStringParser PARSER = new FastQSeqStringParser(PREFIX_LENGTH, (short) -1);

	@BeforeClass
	public static void setUpClass() {
		Configuration.FILE_FORMAT = FileFormat.FILE_FORMAT_FASTQ;
		Configuration.PREFIX_LENGTH = PREFIX_LENGTH;
		Configuration.NUMBER_BASES_COMPARED = -1;
	}

	@AfterClass
	public static void tearDownClass() {
		Configuration.NUMBER_BASES_COMPARED = -1;
	}

	private static String randomString(Random random, String alphabet, int length) {
		StringBuilder sb = new StringBuilder(length);

		for (int i = 0; i < length; i++)
			sb.append(alphabet.charAt(random.nextInt(alphabet.length())));

		return sb.toString();
	}

	private static String record(String name, String bases, String quals) {
		return name + "\n" + bases + "\n+\n" + quals;
	}

	private static Sequence parse(String name, String bases, String quals) {
		Sequence seq = new Sequence();
		PARSER.getSeq(name + "\n" + bases + "\n+\n" + quals, seq, true);
		return seq;
	}

	private static byte[] serialize(Sequence seq) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		seq.write(new DataOutputStream(bytes));
		return bytes.toByteArray();
	}

	private static void readFields(Sequence seq, byte[] data) throws IOException {
		seq.readFields(new DataInputStream(new ByteArrayInputStream(data)));
	}

	@Test
	public void avgQualIsRecomputedWhenTheObjectIsReusedByReadFields() throws IOException {
		byte[] low = serialize(parse("@low", "ACGTACGTACGTACGTACGTACGT", "!!!!!!!!!!!!!!!!!!!!!!!!"));
		byte[] high = serialize(parse("@high", "ACGTACGTACGTACGTACGTACGT", "IIIIIIIIIIIIIIIIIIIIIIII"));
		Sequence reused = new Sequence();

		readFields(reused, low);
		assertEquals('!', reused.getAvgQual(), 0.0001f);

		// Hadoop deserializes every value of a reducer group into the same object
		readFields(reused, high);
		assertEquals('I', reused.getAvgQual(), 0.0001f);
	}

	@Test
	public void avgQualIsRecomputedWhenQualsAreReplaced() {
		Sequence seq = parse("@seq", "ACGTACGTACGTACGTACGTACGT", "!!!!!!!!!!!!!!!!!!!!!!!!");
		assertEquals('!', seq.getAvgQual(), 0.0001f);

		seq.setQuals("IIIIIIIIIIIIIIIIIIIIIIII");
		assertEquals('I', seq.getAvgQual(), 0.0001f);
	}

	@Test
	public void avgQualIsTheMeanOfTheQualityScores() {
		assertEquals(('5' + 'I') / 2.0f, parse("@seq", "ACGTACGTAC", "5I5I5I5I5I").getAvgQual(), 0.0001f);
	}

	@Test
	public void toStringRestoresTheParsedRecord() {
		Random random = new Random(1);

		for (int length : new int[] { 6, 21, 32, 37, 101, 150 }) {
			String expected = record("@read" + length, randomString(random, "ACGTN", length), randomString(random, "!5I", length));
			Sequence seq = new Sequence();
			PARSER.getSeq(expected, seq, true);
			assertEquals(expected, seq.toString());
		}
	}

	@Test
	public void toStringRestoresTheParsedRecordWhenOnlyPartOfTheReadIsCompared() {
		Configuration.NUMBER_BASES_COMPARED = 50;

		try {
			SeqStringParser parser = new FastQSeqStringParser(PREFIX_LENGTH, (short) 50);
			Random random = new Random(2);

			for (int length : new int[] { 40, 50, 55, 101 }) {
				String expected = record("@read" + length, randomString(random, "ACGT", length), randomString(random, "!5I", length));
				Sequence seq = new Sequence();
				parser.getSeq(expected, seq, true);
				assertEquals(expected, seq.toString());
			}
		} finally {
			Configuration.NUMBER_BASES_COMPARED = -1;
		}
	}

	@Test
	public void firstAndSecondReadsWithTheSameSuffixLengthKeepTheirOwnLayout() {
		// The first read has a 50-base suffix of which 45 are compared, the second one has 50 bases of which none is compared
		Configuration.NUMBER_BASES_COMPARED = 50;

		try {
			SeqStringParser firstParser = new FastQSeqStringParser(PREFIX_LENGTH, (short) 50);
			SeqStringParser secondParser = new FastQSeqStringParser((short) 0, (short) 50);
			Random random = new Random(3);
			String first = record("@pair/1", randomString(random, "ACGT", 55), randomString(random, "!5I", 55));
			String second = record("@pair/2", randomString(random, "ACGT", 50), randomString(random, "!5I", 50));
			Sequence firstSeq = new Sequence();
			Sequence secondSeq = new Sequence();

			firstParser.getSeq(first, firstSeq, true);
			secondParser.getSeq(second, secondSeq, false);

			assertEquals(first, firstSeq.toString());
			assertEquals(second, secondSeq.toString());
		} finally {
			Configuration.NUMBER_BASES_COMPARED = -1;
		}
	}

	@Test
	public void sequencesOfDifferentLengthsCanBeProcessedConcurrently() throws Exception {
		// Tasks that share a JVM must not see each other's reads
		final int[] lengths = { 24, 37, 101, 150 };
		ExecutorService executor = Executors.newFixedThreadPool(lengths.length);

		try {
			List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

			for (final int length : lengths) {
				futures.add(executor.submit(new Callable<Integer>() {
					@Override
					public Integer call() {
						Random random = new Random(length);
						SeqStringParser parser = new FastQSeqStringParser(PREFIX_LENGTH, (short) -1);
						Sequence seq = new Sequence();
						int wrong = 0;

						for (int i = 0; i < 20000; i++) {
							String expected = record("@read" + i, randomString(random, "ACGT", length), randomString(random, "!5I", length));
							parser.getSeq(expected, seq, true);

							if (!expected.equals(seq.toString()))
								wrong++;
						}

						return wrong;
					}
				}));
			}

			for (Future<Integer> future : futures)
				assertEquals(0, future.get().intValue());
		} finally {
			executor.shutdownNow();
		}
	}
}
