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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;

public class FastQSeqStringParser extends SeqStringParser {

	public FastQSeqStringParser(short prefixLength, int numBasesCompared) {
		super(prefixLength, numBasesCompared);
	}

	// A well formed record of ASCII characters is read from its bytes. Any other record (a byte above 127, a missing
	// or extra line, qualities of another length, a read shorter than the prefix) goes to the String parser, which
	// decodes it and reports the problem
	@Override
	public int getSeq(byte[] buf, int length, Sequence seq, boolean isFirstSeq) throws CharacterCodingException {
		// OR of all the bytes read: negative if one is above 127
		int ascii = 0;
		int i = 0;

		while (i < length && buf[i] != '\n')
			ascii |= buf[i++];

		int nameEnd = i;
		int basesStart = ++i;

		while (i < length && buf[i] != '\n')
			ascii |= buf[i++];

		int basesEnd = i;
		i++;

		// The comment line is not used
		while (i < length && buf[i] != '\n')
			ascii |= buf[i++];

		int qualsStart = ++i;

		while (i < length && buf[i] != '\n')
			ascii |= buf[i++];

		int qualsEnd = i;
		int numBases = basesEnd - basesStart;

		// The record may end with a line feed
		if (ascii < 0 || qualsStart >= length || qualsEnd < length - 1 || qualsEnd - qualsStart != numBases || numBases < this.getPrefixLength())
			return getSeq(Text.decode(buf, 0, length, false), seq, isFirstSeq);

		seq.setName(new String(buf, 0, nameEnd, StandardCharsets.ISO_8859_1));
		seq.setIsFirstSeq(isFirstSeq);
		seq.setKey(buf, basesStart, numBases, this.getPrefixLength());
		seq.setSuffix(buf, basesStart, numBases, this.getPrefixLength(), this.getNumBasesCompared());
		seq.setQuals(new String(buf, qualsStart, numBases, StandardCharsets.ISO_8859_1));

		return numBases;
	}

	@Override
	public int getSeq(String value, Sequence seq, boolean isFirstSeq) {

		OptimizedStringTokenizer st = new OptimizedStringTokenizer(value, '\n');

		// Read the sequence name
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + value
					+ ": no name available");
		seq.setName(st.nextToken());

		// Read the sequence bases
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + value
					+ ": only name available");
		String bases = st.nextToken();

		// Ignore the comment line
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + value
					+ ": no comment line available");
		st.nextToken();

		// Read the qualities
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + value
					+ ": no qualities available");
		String strQuals = st.nextToken();

		// For base-space reads
		if (strQuals.length() != bases.length())
			throw new IllegalArgumentException("The number of bases ("+bases.length()+") is not equal to the number of quality scores ("+strQuals.length()+")");

		seq.setIsFirstSeq(isFirstSeq);
		seq.setKey(bases, this.getPrefixLength());
		seq.setSuffix(bases, this.getPrefixLength(), this.getNumBasesCompared());
		seq.setQuals(strQuals);

		return bases.length();
	}
}
