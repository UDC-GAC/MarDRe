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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

/**
 * The parsers read the records from the bytes of the Text that the record readers return, and from a String:
 * both entries must produce the same Sequence and fail in the same way.
 */
public class SeqBytesParserTest {

	@Before
	public void setUp() {
		Configuration.FILE_FORMAT = FileFormat.FILE_FORMAT_FASTQ;
		Configuration.OPTICAL_DIST = -1;
	}

	@After
	public void tearDown() {
		Configuration.PREFIX_LENGTH = 20;
		Configuration.NUMBER_BASES_COMPARED = -1;
	}

	private static String randomString(Random random, String alphabet, int length) {
		StringBuilder sb = new StringBuilder(length);

		for (int i = 0; i < length; i++)
			sb.append(alphabet.charAt(random.nextInt(alphabet.length())));

		return sb.toString();
	}

	private static byte[] serialize(Sequence seq) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		seq.write(new DataOutputStream(bytes));
		return bytes.toByteArray();
	}

	private static SeqStringParser parser(int prefix, int compared) {
		Configuration.PREFIX_LENGTH = (short) prefix;
		Configuration.NUMBER_BASES_COMPARED = compared;
		return new FastQSeqStringParser((short) prefix, compared);
	}

	// A buffer larger than the record, as the one of a Text that has held longer records
	private static byte[] buffer(String record, int spare) {
		byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
		byte[] padded = new byte[bytes.length + spare];
		java.util.Arrays.fill(padded, (byte) 'Z');
		System.arraycopy(bytes, 0, padded, 0, bytes.length);
		return padded;
	}

	private static String fromBytes(SeqStringParser parser, String record, Sequence seq, boolean isFirst) throws Exception {
		try {
			int length = parser.getSeq(buffer(record, 7), record.getBytes(StandardCharsets.UTF_8).length, seq, isFirst);
			return "ok " + length;
		} catch (Exception e) {
			return failure(e);
		}
	}

	private static String fromString(SeqStringParser parser, String record, Sequence seq, boolean isFirst) {
		try {
			return "ok " + parser.getSeq(record, seq, isFirst);
		} catch (Exception e) {
			return failure(e);
		}
	}

	private static String failure(Exception e) {
		// The text of an out of range index depends on the JDK
		return (e instanceof IndexOutOfBoundsException)? "IndexOutOfBoundsException" : e.getClass().getName() + ": " + e.getMessage();
	}

	private static void assertSameParsing(SeqStringParser parser, String record, Sequence viaBytes, Sequence viaString, boolean isFirst) throws Exception {
		String expected = fromString(parser, record, viaString, isFirst);
		String actual = fromBytes(parser, record, viaBytes, isFirst);

		assertEquals("record '" + record + "'", expected, actual);

		if (expected.startsWith("ok"))
			assertTrue("record '" + record + "'", java.util.Arrays.equals(serialize(viaString), serialize(viaBytes)));
	}

	@Test
	public void randomRecordsGiveTheSameSequenceThroughBothEntries() throws Exception {
		Random random = new Random(31);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		for (int prefix : new int[] { 1, 5, 20, 27 }) {
			for (int compared : new int[] { -1, 40, 101 }) {
				SeqStringParser parser = parser(prefix, compared);

				for (int i = 0; i < 3000; i++) {
					int length = prefix + random.nextInt(130);
					String name = "@" + randomString(random, "abcXYZ019:_/ .-", random.nextInt(40));
					String comment = random.nextBoolean()? "+" : "+" + name.substring(1);
					String record = name + "\n" + randomString(random, "ACGTNacgtnRYKMSWBDHV", length) + "\n" + comment + "\n"
							+ randomString(random, "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJ", length);
					assertSameParsing(parser, record, viaBytes, viaString, random.nextBoolean());
				}
			}
		}
	}

	@Test
	public void recordsWithNonAsciiCharactersGiveTheSameSequence() throws Exception {
		SeqStringParser parser = parser(5, -1);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		assertSameParsing(parser, "@réad ü€/1\nACGTACGTAC\n+\nIIIIIIIIII", viaBytes, viaString, true);
		// Qualities that take more bytes than characters: the count is of characters
		assertSameParsing(parser, "@r\nACGTACGT\n+\nIIIIééII", viaBytes, viaString, true);
		assertSameParsing(parser, "@r\nACGTACGT\n+\nIIIIIIIé", viaBytes, viaString, true);
		// Non-ASCII in the bases
		assertSameParsing(parser, "@r\nACGTéCGT\n+\nIIIIIIII", viaBytes, viaString, true);
	}

	@Test
	public void aMalformedUtf8NameIsRejectedAsBefore() throws Exception {
		SeqStringParser parser = parser(5, -1);
		byte[] record = "@ré\nACGTACGT\n+\nIIIIIIII".getBytes(StandardCharsets.UTF_8);
		// Truncate the two bytes of the e acute to its first one
		byte[] broken = new byte[record.length - 1];
		System.arraycopy(record, 0, broken, 0, 3);
		System.arraycopy(record, 4, broken, 3, record.length - 4);

		try {
			parser.getSeq(broken, broken.length, new Sequence(), true);
			fail("malformed UTF-8");
		} catch (CharacterCodingException e) {
			// As Text.decode did
		}
	}

	@Test
	public void malformedRecordsFailAsBefore() throws Exception {
		SeqStringParser parser = parser(5, -1);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		String[] records = { "", "@n", "@n\n", "@n\nACGTACGT", "@n\nACGTACGT\n", "@n\nACGTACGT\n+", "@n\nACGTACGT\n+\n",
				"@n\nACGTACGT\n+\nIII", "@n\nACGTACGT\n+\nIIIIIIIII", "@n\nACG1ACGT\n+\nIIIIIIII", "@n\nACGT\n+\nIIII",
				"@n\n\n+\n", "\nACGTACGT\n+\nIIIIIIII", "@n\r\nACGTACGT\r\n+\r\nIIIIIIII\r", "@n\nACGTACGT\n+\nIIIIIIII\nextra\nlines" };

		for (String record : records)
			assertSameParsing(parser, record, viaBytes, viaString, true);
	}

	@Test
	public void aReadShorterThanThePrefixIsRejected() throws Exception {
		SeqStringParser parser = parser(20, -1);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		for (int length = 0; length < 20; length++)
			assertSameParsing(parser, "@n\n" + "A".repeat(length) + "\n+\n" + "I".repeat(length), viaBytes, viaString, true);
	}

	private static SeqStringParser fastaParser(int prefix, int compared) {
		Configuration.PREFIX_LENGTH = (short) prefix;
		Configuration.NUMBER_BASES_COMPARED = compared;
		return new FastASeqStringParser((short) prefix, compared);
	}

	// Bases in lines of random length, possibly with empty lines and a final line feed
	private static String wrap(Random random, String bases) {
		StringBuilder sb = new StringBuilder();
		int pos = 0;

		while (pos < bases.length()) {
			int line = 1 + random.nextInt(bases.length());

			if (random.nextInt(6) == 0)
				line = bases.length();

			sb.append(bases, pos, Math.min(bases.length(), pos + line));
			pos += line;

			if (pos < bases.length() || random.nextBoolean())
				sb.append('\n');

			if (random.nextInt(10) == 0)
				sb.append('\n');
		}

		return sb.toString();
	}

	@Test
	public void randomFastaRecordsGiveTheSameSequenceThroughBothEntries() throws Exception {
		Random random = new Random(32);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		for (int prefix : new int[] { 1, 5, 20, 27 }) {
			for (int compared : new int[] { -1, 40, 101 }) {
				SeqStringParser parser = fastaParser(prefix, compared);

				for (int i = 0; i < 3000; i++) {
					int length = prefix + random.nextInt(130);
					String record = ">" + randomString(random, "abcXYZ019:_/ .-", random.nextInt(40)) + "\n"
							+ wrap(random, randomString(random, "ACGTNacgtnRYKMSWBDHV", length));
					assertSameParsing(parser, record, viaBytes, viaString, random.nextBoolean());
				}
			}
		}
	}

	@Test
	public void fastaRecordsWithNonAsciiCharactersOrMalformedGiveTheSameResult() throws Exception {
		SeqStringParser parser = fastaParser(5, -1);
		Sequence viaBytes = new Sequence();
		Sequence viaString = new Sequence();

		String[] records = { "", ">n", ">n\n", ">n\n\n", ">n\nACGTACGT", ">n\nACGTACGT\n", ">n\nACG\nTACGT\n\nAC", ">n\nAC1TACGT",
				">n\nACG", ">n\r\nACGTACGT\r\n", ">r\u00e9 \u20ac\nACGTACGT", ">n\nACGT\u00e9CGT", "\nACGTACGT" };

		for (String record : records)
			assertSameParsing(parser, record, viaBytes, viaString, true);
	}
}
