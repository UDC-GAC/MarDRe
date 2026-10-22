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

import org.junit.BeforeClass;
import org.junit.Test;

import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class SequenceTest {

	private static final short PREFIX_LENGTH = 5;
	private static final SeqStringParser PARSER = new FastQSeqStringParser(PREFIX_LENGTH, (short) -1);

	@BeforeClass
	public static void setUpClass() {
		Configuration.FILE_FORMAT = FileFormat.FILE_FORMAT_FASTQ;
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
}
