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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class LineReader implements Closeable {

	private int bufferSize;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPos = 0;

	// Line delimiter
	private static final byte LF = '\n';

	public LineReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	public void close() throws IOException {
		in.close();
	}

	/**
	 * Seek to the given offset.
	 *
	 * @param pos offset to seek to
	 *
	 * @throws IOException
	 */
	public void seek(long pos) throws IOException {
		if (pos < 0 || pos >= bufferSize)
			throw new IOException("Incorrect position: "+pos);

		this.bufferPos = (int) pos;
	}

	/**
	 * Return the current offset from the start of the file
	 *
	 * @throws IOException
	 */
	public long getPos() throws IOException {
		return bufferPos;
	}

	/**
	 * Read a line from the InputStream terminated by LF into the given Text.
	 *
	 * @param str the object to store the given line (including the newline) 
	 *
	 * @return the number of bytes read including the newline
	 *
	 * @throws IOException if the underlying stream throws
	 */
	public int readLine(Text str) throws IOException {
		/* We're reading data from in, but the head of the stream may be
		 * already buffered, so we have two cases:
		 * 1. No newline characters are in the buffer, so we need to copy
		 *    everything and read another buffer from the stream.
		 * 2. An unambiguously terminated line is in buffer, so we just
		 *    copy to str.
		 */
		str.clear();
		int txtLength = 0; //tracks str.getLength(), as an optimization
		boolean newLine = false;
		long bytesConsumed = 0;
		int readLength = 0;
		int startPos = 0;

		do {
			startPos = bufferPos; //starting from where we left off the last time
			if (bufferPos >= bufferLength) {
				startPos = bufferPos = 0;
				bufferLength = in.read(buffer);
				if (bufferLength <= 0) {
					break; // EOF
				}
			}
			for (; bufferPos < bufferLength; ++bufferPos) { //search for newline
				if (buffer[bufferPos] == LF) {
					newLine = true;
					++bufferPos; // at next invocation proceed from following byte
					break;
				}
			}
			readLength = bufferPos - startPos;
			bytesConsumed += readLength;
			if (readLength == 1 && newLine && txtLength == 0) {
				// Only one byte was read and it is the line delimiter (LF)
				newLine = false;
			} else {
				str.append(buffer, startPos, readLength);
				txtLength += readLength;
			}
		} while (!newLine);

		return (int)bytesConsumed;
	}
}
