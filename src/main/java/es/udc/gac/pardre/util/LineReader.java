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
	 * Read one line from the InputStream into the given Text.
	 *
	 * @param str the object to store the given line (including the newline)
	 * @param maxLineLength the maximum number of bytes to store into str;
	 *  the rest of the line is silently discarded.
	 *
	 * @return the number of bytes read including the (longest) newline
	 * found.
	 *
	 * @throws IOException if the underlying stream throws
	 */
	public int readLine(Text str, int maxLineLength) throws IOException {
		return readDefaultLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	/**
	 * Read from the InputStream into the given Text.
	 *
	 * @param str the object to store the given line (including the newline) 
	 *
	 * @return the number of bytes read including the newline
	 *
	 * @throws IOException if the underlying stream throws
	 */
	public int readLine(Text str) throws IOException {
		return readDefaultLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}

	private int fillBuffer(InputStream in, byte[] buffer)
			throws IOException {
		return in.read(buffer);
	}

	/**
	 * Read a line terminated by LF.
	 */
	private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume)
			throws IOException {
		/* We're reading data from in, but the head of the stream may be
		 * already buffered, so we have two cases:
		 * 1. No newline characters are in the buffer, so we need to copy
		 *    everything and read another buffer from the stream.
		 * 2. An unambiguously terminated line is in buffer, so we just
		 *    copy to str.
		 */
		str.clear();
		int txtLength = 0; //tracks str.getLength(), as an optimization
		int newlineLength = 0; //length of terminating newline
		long bytesConsumed = 0;
		int readLength = 0;
System.out.println("Init: bufferPos="+bufferPos);
		do {
			int startPosn = bufferPos; //starting from where we left off the last time
			if (bufferPos >= bufferLength) {
				startPosn = bufferPos = 0;
				bufferLength = fillBuffer(in, buffer);
				if (bufferLength <= 0) {
					break; // EOF
				}
			}
System.out.println("Before for: bufferPos="+bufferPos);
			for (; bufferPos < bufferLength; ++bufferPos) { //search for newline
				if (buffer[bufferPos] == LF) {
					newlineLength = 1;
					++bufferPos; // at next invocation proceed from following byte
					break;
				}
			}
System.out.println("After for: bufferPos="+bufferPos);
			readLength = bufferPos - startPosn;
			bytesConsumed += readLength;
			if (readLength > maxLineLength - txtLength) {
				readLength = maxLineLength - txtLength;
			}
			if (readLength > 0) {
				str.append(buffer, startPosn, readLength);
				txtLength += readLength;
			}
		} while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);
System.out.println("End: bufferPos="+bufferPos);
		if (bytesConsumed > Integer.MAX_VALUE) {
			throw new IOException("Too many bytes before newline: " +bytesConsumed);
		}
		return (int)bytesConsumed;
	}
}
