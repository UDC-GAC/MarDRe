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

public class FastASeqStringParser extends SeqStringParser {

	public FastASeqStringParser(short prefixLength, int numBasesCompared) {
		super(prefixLength, numBasesCompared);
	}

	// The bases of the record, without its line feeds, when they are in several lines
	private byte[] joined = new byte[0];

	// A record of ASCII characters is read from its bytes. Any other record (a byte above 127, no bases, a read
	// shorter than the prefix) goes to the String parser, which decodes it and reports the problem
	@Override
	public int getSeq(byte[] buf, int length, Sequence seq, boolean isFirstSeq) throws CharacterCodingException {
		int ascii = 0;
		int i = 0;

		while (i < length && buf[i] != '\n')
			ascii |= buf[i++];

		int nameEnd = i;
		int basesStart = ++i;

		// The bases are the rest of the record, whose line feeds are not bases
		byte[] bases = buf;
		int start = basesStart;
		int numBases = 0;

		if (basesStart < length) {
			int lineFeeds = 0;

			for (i = basesStart; i < length; i++) {
				byte b = buf[i];
				ascii |= b;

				if (b == '\n')
					lineFeeds++;
			}

			numBases = length - basesStart - lineFeeds;

			if (lineFeeds > 0 && ascii >= 0) {
				if (joined.length < numBases)
					joined = new byte[Math.max(numBases, 2 * joined.length)];

				int n = 0;

				for (i = basesStart; i < length; i++) {
					if (buf[i] != '\n')
						joined[n++] = buf[i];
				}

				bases = joined;
				start = 0;
			}
		}

		if (ascii < 0 || basesStart >= length || numBases < this.getPrefixLength())
			return getSeq(Text.decode(buf, 0, length, false), seq, isFirstSeq);

		seq.setName(new String(buf, 0, nameEnd, StandardCharsets.ISO_8859_1));
		seq.setIsFirstSeq(isFirstSeq);
		seq.setKey(bases, start, numBases, this.getPrefixLength());
		seq.setSuffix(bases, start, numBases, this.getPrefixLength(), this.getNumBasesCompared());

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
