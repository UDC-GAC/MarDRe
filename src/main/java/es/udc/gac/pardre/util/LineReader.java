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
	private int bufferPosn = 0;

	// Line delimiters
	private static final byte CR = '\r';
	private static final byte LF = '\n';
	private static final byte[] LF_end = { '\n' };

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

	public int getBufferPosn() {
		return bufferPosn;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	private int fillBuffer(InputStream in, byte[] buffer)
			throws IOException {
		return in.read(buffer);
	}

	/**
	 * Read a line terminated by one of CR, LF, or CRLF.
	 */
	private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume)
			throws IOException {
		/* We're reading data from in, but the head of the stream may be
		 * already buffered, so we have several cases:
		 * 1. No newline characters are in the buffer, so we need to copy
		 *    everything and read another buffer from the stream.
		 * 2. An unambiguously terminated line is in buffer, so we just
		 *    copy to str.
		 * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
		 *    in CR.  In this case we copy everything up to CR to str, but
		 *    we also need to see what follows CR: if it's LF, then we
		 *    need consume LF as well, so next call to readLine will read
		 *    from after that.
		 * We use a flag prevCharCR to signal if previous character was CR
		 * and, if it happens to be at the end of the buffer, delay
		 * consuming it until we have a chance to look at the char that
		 * follows.
		 */
		str.clear();
		int txtLength = 0; //tracks str.getLength(), as an optimization
		int newlineLength = 0; //length of terminating newline
		boolean prevCharCR = false; //true of prev char was CR
		long bytesConsumed = 0;

		do {
			int startPosn = bufferPosn; //starting from where we left off the last time
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				if (prevCharCR) {
					++bytesConsumed; //account for CR from previous read
				}
				bufferLength = fillBuffer(in, buffer);
				if (bufferLength <= 0) {
					break; // EOF
				}
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
				if (buffer[bufferPosn] == LF) {
					newlineLength = (prevCharCR) ? 2 : 1;
					++bufferPosn; // at next invocation proceed from following byte
					break;
				}
				if (prevCharCR) { //CR + notLF, we are at notLF
					newlineLength = 1;
					break;
				}
				prevCharCR = (buffer[bufferPosn] == CR);
			}
			int readLength = bufferPosn - startPosn;
			if (prevCharCR && newlineLength == 0) {
				--readLength; //CR at the end of the buffer
			}
			bytesConsumed += readLength;
			int appendLength = readLength - newlineLength;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
		} while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

		// Newline
		str.append(LF_end, 0, 1);

		if (bytesConsumed > Integer.MAX_VALUE) {
			throw new IOException("Too many bytes before newline: " + bytesConsumed);
		}
		return (int)bytesConsumed;
	}
}
