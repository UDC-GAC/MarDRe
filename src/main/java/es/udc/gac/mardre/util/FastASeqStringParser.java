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

public class FastASeqStringParser extends SeqStringParser {

	public FastASeqStringParser(short prefixLength, int numBasesCompared) {
		super(prefixLength, numBasesCompared);
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

		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + value
					+ ": only name available");

		// Read the sequence bases
		String bases = new String();
		while(st.hasMoreTokens())
			bases += st.nextToken();

		seq.setIsFirstSeq(isFirstSeq);
		seq.setKey(bases, this.getPrefixLength());
		seq.setSuffix(bases, this.getPrefixLength(), this.getNumBasesCompared());

		return bases.length();
	}
}
