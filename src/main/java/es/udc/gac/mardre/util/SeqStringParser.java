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

public abstract class SeqStringParser {

	private final short prefixLength;
	private final int numBasesCompared;

	public SeqStringParser(short prefixLength, int numBasesCompared) {
		this.prefixLength = prefixLength;
		this.numBasesCompared = numBasesCompared;
	}

	public short getPrefixLength() {
		return prefixLength;
	}

	public int getNumBasesCompared() {
		return numBasesCompared;
	}

	public abstract int getSeq(String s, Sequence seq, boolean isFirstSeq);
}