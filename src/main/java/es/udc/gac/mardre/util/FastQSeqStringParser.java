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
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

public class FastQSeqStringParser extends SeqStringParser {

	public FastQSeqStringParser(short prefixLength, short numBasesCompared, boolean isSecondPairedRead) {
		super(prefixLength, numBasesCompared, isSecondPairedRead);
	}

	@Override
	public int getSeq(String value, Sequence seq) {

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

		seq.setKey(bases, this.getPrefixLength());
		seq.setSuffix(bases, this.getPrefixLength(), this.getNumBasesCompared(), this.isSecondPairedRead());
		seq.setQuals(strQuals);

		return bases.length();
	}
}
