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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class LineReader implements Closeable {

	private static final byte LF = '\n'; // Line delimiter

	private int bufferSize;
	private InputStream inputStream;
	private byte[] buffer;
	private int bufferLength; // the number of bytes of real data in the buffer
	private int bufferPos; 	// the current position in the buffer

	public LineReader(InputStream inputStream, org.apache.hadoop.conf.Configuration conf) {
		this.inputStream = inputStream;
		this.bufferSize = Configuration.getIOFileBufferSize(conf);
		this.buffer = new byte[this.bufferSize];
		this.bufferPos = 0;
		this.bufferLength = 0;
	}

	public void close() throws IOException {
		inputStream.close();
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
	 * Return the current offset from the start of the buffer.
	 */
	public long getPos() {
		return bufferPos;
	}

	/**
	 * Read a line from the InputStream terminated by LF into the given Text.
	 *
	 * @param str the object to store the given line (including LF) 
	 *
	 * @return the number of bytes read including LF
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
				bufferLength = inputStream.read(buffer);

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

		return (int) bytesConsumed;
	}

	/**
	 * Finds the first occurence of <code>what</code> in the backing
	 * buffer of <code>text</code>, starting as position 
	 * <code>start</code>, and then trims <code>text</code>.
	 * The starting position is measured in bytes.
	 * 
	 * @param text Text to trim
	 * @param what Character to find in text
	 * @param start Starting byte position
	 */
	public static void trim(Text text, char what, int start) {
		int pos = text.find(String.valueOf(what), start);

		if (pos != -1) {
			text.getBytes()[pos] = LF;
			text.set(text.getBytes(), 0, ++pos);
		}
	}
}
