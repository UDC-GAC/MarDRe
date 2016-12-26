/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MaRDRe.
 * 
 * MaRDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MaRDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MaRDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import java.util.StringTokenizer;

import es.udc.gac.mardre.util.SequenceFactory.FileFormat;

public class FastQSeqStringParser extends SeqStringParser {

	public FastQSeqStringParser(short prefixLength, char minQual) {
		super(prefixLength, minQual);
	}

	@Override
	public int getSeq(String s, Sequence seq) {

		if (s.equals(""))
			throw new IllegalArgumentException("Found empty string " + s);

		StringTokenizer st = new StringTokenizer(s, "\n");
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no name available");
		String str = st.nextToken().split("\\s+")[0];

		// Read the sequence name
		// Everything but the '@'
		seq.setName(str.substring(1));

		// Read the sequence bases
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");
		str = st.nextToken();

		// Read the comment line
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no comment available");
		st.nextToken();

		// Read the qualities
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no qualities available");
		String strQuals = st.nextToken();

		seq.setFormat(FileFormat.FILE_FORMAT_FASTQ);
		seq.setLength(str.length());
		seq.setPrefixLength(this.get_prefixLength());
		seq.setQuals(strQuals);
		seq.setKey(str, this.get_minQual());
		seq.setSuffix(str, this.get_minQual());

		return seq.getLength();
	}
}