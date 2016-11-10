package es.udc.gac.pardre.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class LineReader implements Closeable {

	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;	//64 KB
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPos = 0;

	// Line delimiter
	private static final byte LF = '\n';

	public LineReader(InputStream in) {
		this(in, DEFAULT_BUFFER_SIZE);
	}

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
