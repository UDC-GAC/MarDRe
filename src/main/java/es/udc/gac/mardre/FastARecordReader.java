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
package es.udc.gac.mardre;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import es.udc.gac.mardre.util.LineReader;

public class FastARecordReader extends SequenceRecordReader {

	private Text newLine;
	private long numReads;

	public FastARecordReader() {
		super();
		newLine = new Text();
		this.numReads = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		long read = 0;
		boolean found = false;
		value.clear();

		//System.out.println("nextKeyValue: start "+start+", end "+end+", splitPos "+getSplitPosition());

		if (isSplitFinished())
			return false;

		while (true) {
			read = readLine(newLine);

			//System.out.println("nextKeyValue: read "+read+", start "+start+", end "+end+", splitPos "+getSplitPosition());

			if (read == 0) {
				// EOF
				if (found)
					break;
				return false;
			}

			if (newLine.charAt(0) == '>') {
				//System.out.println("nextKeyValue: starting '>' has been found");

				if (found) {
					seek(getLineReaderPosition() - read);
					pos -= read;
					break;
				} else {
					numReads++;
					key.set(start+numReads);
					//Trim spaces in sequence name
					LineReader.trim(newLine, ' ', 1);
					found = true;
				}
			}

			if (found)
				value.append(newLine.getBytes(), 0, newLine.getLength());
		}

		//System.out.println("nextKeyValue: start "+start+", end "+end+", splitPos "+getSplitPosition());

		return true;
	}
}
