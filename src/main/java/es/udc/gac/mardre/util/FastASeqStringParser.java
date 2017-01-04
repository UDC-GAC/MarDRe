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

public class FastASeqStringParser extends SeqStringParser {

	public FastASeqStringParser(short prefixLength, char minQual) {
		super(prefixLength, minQual);
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
		String name = st.nextToken().split("\\s+")[0];
		seq.setName(name);

		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");

		// Read the sequence bases
		String bases = new String();
		while(st.hasMoreTokens())
			bases += st.nextToken();

		seq.setLength(bases.length());
		seq.setPrefixLength(this.getPrefixLength());
		seq.setKey(bases);
		seq.setSuffix(bases);

		return seq.getLength();
	}
}