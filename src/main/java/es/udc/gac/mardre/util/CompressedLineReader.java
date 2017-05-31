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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

public class CompressedLineReader extends LineReader {

	private SplitCompressionInputStream scIn;
	private boolean finished = false;

	public CompressedLineReader(SplitCompressionInputStream in, int bufferSize) {
		super(in, bufferSize);
		this.scIn = in;
	}

	@Override
	public int readLine(Text str) throws IOException {
		int bytesRead = 0;

		if (!finished) {
			// only allow at most one more record to be read after the stream
			// reports the split ended
			if (scIn.getPos() > scIn.getAdjustedEnd())
				finished = true;

			bytesRead = super.readLine(str);
		}
		return bytesRead;
	}
}
