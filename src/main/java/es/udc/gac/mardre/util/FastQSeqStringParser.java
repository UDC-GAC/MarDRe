/*
 * Copyright (C) 2017 Universidade da Coruña
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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

public class FastQSeqStringParser extends SeqStringParser {

	public FastQSeqStringParser(short prefixLength) {
		super(prefixLength);
	}

	@Override
	public int getSeq(String s, Sequence seq) {

		if (s.equals(""))
			throw new IllegalArgumentException("Found empty string " + s);

		StringTokenizer st = new StringTokenizer(s, "\n");

		// Read the sequence name
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no name available");
		seq.setName(st.nextToken().split("\\s+")[0]);

		// Read the sequence bases
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");
		String str = st.nextToken();

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

		// For base-space reads
		if (strQuals.length() != str.length())
			throw new IllegalArgumentException("The number of bases is not equal to the number of quality scores");

		seq.setLength(str.length());
		seq.setQuals(strQuals.getBytes(StandardCharsets.US_ASCII));
		seq.setKey(str, this.getPrefixLength());
		seq.setSuffix(str,this.getPrefixLength());

		return seq.getLength();
	}
}
