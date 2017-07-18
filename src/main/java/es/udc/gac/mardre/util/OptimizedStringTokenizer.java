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

public final class OptimizedStringTokenizer {

	private final String str;
	private final char delim;
	private final int len;
	private int pos;

	public OptimizedStringTokenizer(String str, char delim) {
		this.str = str;
		this.pos = 0;
		this.len = str.length();
		this.delim = delim;
	}

	public final boolean hasMoreTokens() {
		return pos < len;
	}

	public final String nextToken() {
		int init = pos;

		while(pos < len && str.charAt(pos) != delim)
			pos++;

		// Skip delimiter
		pos++;
		return str.substring(init, pos-1);
	}
}