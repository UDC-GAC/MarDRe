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

import org.apache.hadoop.io.Text;

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

	/**
	 * Parses the record held in the first 'length' bytes of 'buf', the bytes of the Text that the record readers
	 * return. The parsers override it to read the bytes without decoding the record into a String.
	 */
	public int getSeq(byte[] buf, int length, Sequence seq, boolean isFirstSeq) throws CharacterCodingException {
		return getSeq(Text.decode(buf, 0, length, false), seq, isFirstSeq);
	}
}