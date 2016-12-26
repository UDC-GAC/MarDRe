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

public abstract class SeqStringParser {

	private short _prefixLength;
	private char _minQual;

	public SeqStringParser(short prefixLength, char minQual) {
		this._prefixLength = prefixLength;
		this._minQual = minQual;
	}

	public short get_prefixLength() {
		return _prefixLength;
	}

	public void set_prefixLength(short _prefixLength) {
		this._prefixLength = _prefixLength;
	}

	public char get_minQual() {
		return _minQual;
	}

	public void set_minQual(char _minQual) {
		this._minQual = _minQual;
	}

	public abstract int getSeq(String s, Sequence seq);
}