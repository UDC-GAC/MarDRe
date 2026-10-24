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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.junit.After;
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

	@After
	public void switchOpticalDuplicatesOff() {
		Configuration.OPTICAL_DIST = -1;
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

	private static Sequence read(String name, String quals) {
		Sequence seq = new Sequence();
		PARSER.getSeq(record(name, "ACGTAAAAAAAAAAAAAAAAAAAA".substring(0, quals.length()), quals), seq, true);
		return seq;
	}

	@Test
	public void aReadWithHigherQualityIsBetterWhateverTheName() {
		Sequence high = read("@z", "IIIIIIIIII");
		Sequence low = read("@a", "5555555555");

		assertTrue(high.isBetterThan(low));
		assertFalse(low.isBetterThan(high));
	}

	@Test
	public void withTheSameQualityTheLowestNameIsBetter() {
		Sequence a = read("@a", "5555555555");
		Sequence b = read("@b", "5555555555");

		assertTrue(a.isBetterThan(b));
		assertFalse(b.isBetterThan(a));
	}

	@Test
	public void withTheSameAverageQualityTheNameDecides() {
		// Different scores, same average
		Sequence a = read("@b", "5?5?5?5?5?");
		Sequence b = read("@a", "?5?5?5?5?5");

		assertTrue(b.isBetterThan(a));
		assertFalse(a.isBetterThan(b));
	}

	@Test
	public void aReadIsNotBetterThanItselfOrThanAnotherWithTheSameNameAndQuality() {
		Sequence a = read("@a", "5555555555");

		assertFalse(a.isBetterThan(a));
		assertFalse(a.isBetterThan(read("@a", "5555555555")));
	}

	@Test
	public void readsWithoutQualitiesAreComparedByName() {
		Sequence a = new Sequence();
		a.setName(">a");
		Sequence b = new Sequence();
		b.setName(">b");

		assertTrue(a.isBetterThan(b));
		assertFalse(b.isBetterThan(a));
	}

	@Test
	public void theQualityIsNotStaleAfterReusingTheObject() throws IOException {
		Sequence reused = new Sequence();
		Sequence low = read("@a", "5555555555");
		Sequence high = read("@b", "IIIIIIIIII");

		reused.readFields(new DataInputStream(new ByteArrayInputStream(serialize(low))));
		assertFalse(reused.isBetterThan(high));
		reused.readFields(new DataInputStream(new ByteArrayInputStream(serialize(high))));
		assertTrue(reused.isBetterThan(low));
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
	public void aLongReadSurvivesTheSerialization() throws IOException {
		Random random = new Random(4);

		// More than 127 longs in the compared part (2032 bases), and a length that does not fit in a short
		for (int length : new int[] { 2100, 5000, 40000 }) {
			String expected = record("@long" + length, randomString(random, "ACGTN", length), randomString(random, "!5I", length));
			Sequence seq = new Sequence();
			PARSER.getSeq(expected, seq, true);

			Sequence copy = new Sequence();
			readFields(copy, serialize(seq));

			assertEquals(expected, copy.toString());
		}
	}

	@Test
	public void aLongReadWithOnlyPartOfItComparedSurvivesTheSerialization() throws IOException {
		Configuration.NUMBER_BASES_COMPARED = 100;

		try {
			SeqStringParser parser = new FastQSeqStringParser(PREFIX_LENGTH, (short) 100);
			Random random = new Random(5);

			// The part that is not compared (the remainder) has more than 127 longs too
			for (int length : new int[] { 2300, 40000 }) {
				String expected = record("@long" + length, randomString(random, "ACGT", length), randomString(random, "!5I", length));
				Sequence seq = new Sequence();
				parser.getSeq(expected, seq, true);

				Sequence copy = new Sequence();
				readFields(copy, serialize(seq));

				assertEquals(expected, copy.toString());
			}
		} finally {
			Configuration.NUMBER_BASES_COMPARED = -1;
		}
	}

	@Test
	public void theDistanceBetweenLongReadsIsNotTruncated() {
		String bases = randomString(new Random(6), "ACGT", PREFIX_LENGTH);
		StringBuilder a = new StringBuilder(bases);
		StringBuilder c = new StringBuilder(bases);
		for (int i = 0; i < 40000; i++) {
			a.append('A');
			c.append('C');
		}
		String quals = randomString(new Random(7), "5", 40000 + PREFIX_LENGTH);

		int distance = parse("@a", a.toString(), quals).setDist(parse("@c", c.toString(), quals));

		assertEquals(40000, distance);
	}

	private static final String BASES = "ACGTAAAAAAAAAAAAAAAAAAAA";

	private static Sequence named(String name, boolean isFirstSeq) {
		Sequence seq = new Sequence();
		PARSER.getSeq(record(name, BASES, "I".repeat(BASES.length())), seq, isFirstSeq);
		return seq;
	}

	private static Sequence named(String name) {
		return named(name, true);
	}

	private static String casava(int tile, int x, int y) {
		return "@K00417:10:HHKL2BBXX:1:" + tile + ":" + x + ":" + y + " 1:N:0:CGGCTATG+AGGCTATA";
	}

	/** The reads that are in the same tile are close if their euclidean distance, truncated to an integer, is at most the one of -d. */
	@Test
	public void readsOfTheSameTileAreCloseUpToTheOpticalDistance() {
		Configuration.OPTICAL_DIST = 50;

		assertTrue(named(casava(1101, 1000, 2000)).isOpticallyClose(named(casava(1101, 1030, 2040))));
		assertTrue(named(casava(1101, 1030, 2040)).isOpticallyClose(named(casava(1101, 1000, 2000))));

		Configuration.OPTICAL_DIST = 49;

		assertFalse(named(casava(1101, 1000, 2000)).isOpticallyClose(named(casava(1101, 1030, 2040))));
	}

	@Test
	public void theDistanceIsTruncatedToAnInteger() {
		Configuration.OPTICAL_DIST = 1;

		// sqrt(2) = 1.41: close with -d 1
		assertTrue(named(casava(1101, 10, 10)).isOpticallyClose(named(casava(1101, 11, 11))));
		// sqrt(8) = 2.83: truncated to 2, which is more than 1
		assertFalse(named(casava(1101, 10, 10)).isOpticallyClose(named(casava(1101, 12, 12))));
	}

	@Test
	public void readsOfDifferentTilesLanesOrFlowcellsAreNeverClose() {
		Configuration.OPTICAL_DIST = 1000000;
		Sequence read = named("@K00417:10:HHKL2BBXX:1:1101:1000:2000 1:N:0:AAAA");

		assertTrue(read.isOpticallyClose(named("@K00417:10:HHKL2BBXX:1:1101:1001:2001 1:N:0:AAAA")));
		assertFalse(read.isOpticallyClose(named("@K00417:10:HHKL2BBXX:1:1102:1001:2001 1:N:0:AAAA")));
		assertFalse(read.isOpticallyClose(named("@K00417:10:HHKL2BBXX:2:1101:1001:2001 1:N:0:AAAA")));
		assertFalse(read.isOpticallyClose(named("@K00417:10:HHKL2BBYY:1:1101:1001:2001 1:N:0:AAAA")));
	}

	@Test
	public void whatFollowsTheCoordinatesIsNotPartOfTheTileOrTheDistance() {
		Configuration.OPTICAL_DIST = 10;

		assertTrue(named("@K00417:10:HHKL2BBXX:1:1101:1000:2000 1:N:0:AAAA").isOpticallyClose(named("@K00417:10:HHKL2BBXX:1:1101:1003:2004 2:N:0:CCCC")));
		assertTrue(named("@HWI:268:H7LHBADXX:1:1101:1016:2094/1").isOpticallyClose(named("@HWI:268:H7LHBADXX:1:1101:1020:2097/2")));
	}

	/** The coordinates of the names of the SRA, where the first word is the identifier of the read, are in the second word. */
	@Test
	public void theCoordinatesCanBeInTheSecondWordOfTheName() {
		Configuration.OPTICAL_DIST = 5;
		Sequence read = named("@DRR016102.1 HWI-ST500:268:H7LHBADXX:1:1101:1016:2094/1");

		// The identifier of the read differs and it is not part of the tile
		assertTrue(read.isOpticallyClose(named("@DRR016102.2 HWI-ST500:268:H7LHBADXX:1:1101:1020:2097/1")));
		assertFalse(read.isOpticallyClose(named("@DRR016102.2 HWI-ST500:268:H7LHBADXX:1:1101:1021:2098/1")));
		assertFalse(read.isOpticallyClose(named("@DRR016102.3 HWI-ST500:268:H7LHBADXX:1:1102:1020:2097/1")));
	}

	@Test
	public void theCoordinatesCanBeZeroOrLarge() {
		Configuration.OPTICAL_DIST = 3;

		assertTrue(named(casava(1101, 0, 0)).isOpticallyClose(named(casava(1101, 2, 2))));
		assertTrue(named(casava(1101, 999999999, 999999999)).isOpticallyClose(named(casava(1101, 999999998, 999999999))));
		assertFalse(named(casava(1101, 0, 999999999)).isOpticallyClose(named(casava(1101, 0, 0))));
	}

	@Test
	public void theLastTwoNumbersOfTheWordAreTheCoordinates() {
		Configuration.OPTICAL_DIST = 3;

		// X=30, Y=40 and X=31, Y=41, in a tile that is whatever precedes them
		assertTrue(named("@a:1:2:30:40").isOpticallyClose(named("@a:1:2:31:41")));
		assertFalse(named("@a:1:2:30:40").isOpticallyClose(named("@a:9:2:31:41")));
	}

	@Test
	public void numbersThatDoNotFitInAnIntAreNotCoordinates() {
		Configuration.OPTICAL_DIST = 10;

		try {
			named("@r:1234567890:1");
			fail("coordinates with ten digits");
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("@r:1234567890:1"));
		}

		// Nine digits do
		assertTrue(named("@r:123456789:987654321").isOpticallyClose(named("@r:123456789:987654322")));
	}

	@Test
	public void withoutOpticalDistanceEveryReadIsClose() {
		assertTrue(named("@a").isOpticallyClose(named("@b:1:2")));
	}

	@Test
	public void aNameWithoutCoordinatesIsRejectedNamingTheRead() {
		Configuration.OPTICAL_DIST = 10;

		for (String name : new String[] { "@read1", "@read1 comment", "@a:1 b:2", "@r:12:ab", "@r:1.5:2" }) {
			try {
				named(name);
				fail("no coordinates in " + name);
			} catch (IllegalArgumentException e) {
				assertTrue(e.getMessage(), e.getMessage().contains(name));
			}
		}
	}

	@Test
	public void theSecondMateDoesNotNeedCoordinates() {
		Configuration.OPTICAL_DIST = 10;

		named("@read1", false);
	}

	@Test
	public void theCoordinatesFollowTheObjectWhenItIsReusedByReadFields() throws IOException {
		Configuration.OPTICAL_DIST = 10;
		Sequence a = named(casava(1101, 100, 100));
		Sequence b = named(casava(1101, 500, 500));
		byte[] dataA = serialize(a);
		byte[] dataB = serialize(b);

		Sequence reused = new Sequence();
		readFields(reused, dataA);
		assertTrue(reused.isOpticallyClose(a));
		assertFalse(reused.isOpticallyClose(b));

		readFields(reused, dataB);
		assertTrue(reused.isOpticallyClose(b));
		assertFalse(reused.isOpticallyClose(a));
	}

	@Test
	public void theCoordinatesAreCopiedByTheCopyConstructorAndSet() {
		Configuration.OPTICAL_DIST = 10;
		Sequence a = named(casava(1101, 100, 100));
		Sequence b = named(casava(1101, 500, 500));

		assertTrue(new Sequence(a).isOpticallyClose(a));
		assertFalse(new Sequence(a).isOpticallyClose(b));

		Sequence copy = new Sequence();
		copy.set(b);
		assertTrue(copy.isOpticallyClose(b));
		assertFalse(copy.isOpticallyClose(a));
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

	@Test
	public void theBasesOfEveryPrefixLengthSurviveTheDecoding() {
		Random random = new Random(21);

		try {
			// The key of the prefix holds one base per digit in base 5: up to 27 bases fit in a long
			for (short prefix = 1; prefix <= 27; prefix++) {
				Configuration.PREFIX_LENGTH = prefix;
				SeqStringParser parser = new FastQSeqStringParser(prefix, (short) -1);

				for (int i = 0; i < 2000; i++) {
					// All the bases of the prefix may be the highest digit: N
					String bases = (i % 10 == 0)? "N".repeat(101) : randomString(random, "ACGTN", 101);
					String expected = record("@read" + i, bases, randomString(random, "!5I", 101));
					Sequence seq = new Sequence();
					parser.getSeq(expected, seq, true);

					assertEquals("prefix " + prefix, expected, seq.toString());
				}
			}
		} finally {
			Configuration.PREFIX_LENGTH = PREFIX_LENGTH;
		}
	}
}
