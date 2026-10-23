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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class Sequence implements Writable {

	private static final String COMMENT_LINE = "\n+\n";
	private static final String LF = "\n";

	private static final short[] decodePrefixTab = { 'A', 'C', 'G', 'T', 'N' };

	private static final short[] codePrefixTab = {
			0, 4, 1, 4, 4, 4, 2, // A -> G
			4, 4, 4, 4, 4, 4, 4, // H->N
			4, 4, 4, 4, 4, 3, 4, // O->U
			4, 4, 4, 4, 4 // V->Z
	};

	private static final short[] decodeTab = { 'N', 'A', 'C', 'N', 'G', 'N', 'N', 'N', 'T' };

	private static final short[] codeTab = {
			1, 0, 2, 0, 0, 0, 4, // A -> G
			0, 0, 0, 0, 0, 0, 0, // H->N
			0, 0, 0, 0, 0, 8, 0, // O->U
			0, 0, 0, 0, 0 // V->Z
	};

	private static short codePrefix(char ch) {
		short aux = (short) ch;

		if (ch >= 'A' && ch <= 'Z') {
			aux -= 'A';
		} else if (ch >= 'a' && ch <= 'z') {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected character " + ch 
					+ " in the bases");
		}
		return Sequence.codePrefixTab[aux];
	}

	private static short code(char ch) {
		short aux = (short) ch;

		if (ch >= 'A' && ch <= 'Z') {
			aux -= 'A';
		} else if (ch >= 'a' && ch <= 'z') {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected character " + ch 
					+ " in the bases");
		}
		return Sequence.codeTab[aux];
	}

	private long _key;
	private int _length;
	private int _dist;
	private long[] _compressSuffix;
	private int _compressSuffixLength;
	private long[] _compressSuffixRemainder;
	private boolean _isFirstSeq;
	private String _name;
	private String _quals;
	private float _avgQual;

	// Optical coordinates of a first read, taken from its name (only with -d)
	private String _coordinatesOf;
	private int _baseStart;
	private int _baseEnd;
	private int _x;
	private int _y;

	public Sequence() {
		_name = null;
		_key = 0;
		_length = -1;
		_compressSuffix = null;
		_compressSuffixLength = -1;
		_compressSuffixRemainder = null;
		_isFirstSeq = true;
		_quals = null;
		_avgQual = -1;
		_dist = -1;
	}

	public Sequence(Sequence s) {
		set(s);
	}

	public void set(Sequence s) {
		_name = s._name;
		_key = s._key;
		_length = s._length;
		_compressSuffix = s._compressSuffix;
		_compressSuffixLength = s._compressSuffixLength;
		_compressSuffixRemainder = s._compressSuffixRemainder;
		_isFirstSeq = s._isFirstSeq;
		_quals = s._quals;
		_avgQual = s._avgQual;
		_dist = s._dist;
		_coordinatesOf = s._coordinatesOf;
		_baseStart = s._baseStart;
		_baseEnd = s._baseEnd;
		_x = s._x;
		_y = s._y;
	}

	public void setName(String name) {
		_name = name;
	}

	public void setIsFirstSeq(boolean val) {
		_isFirstSeq = val;

		// Fail as soon as possible if the read cannot be compared by position
		if (val && Configuration.OPTICAL_DIST > 0)
			setCoordinates();
	}

	/**
	 * Takes the coordinates of the read in the flow cell from its name: the first word that ends in two numeric
	 * fields (":X:Y"), which can be followed by something that is not a number (such as "/1"). The tile is what
	 * comes before them in that word, so the identifier that the SRA puts in the first word is not part of it.
	 */
	private void setCoordinates() {
		if (_coordinatesOf == _name)
			return;

		int end = _name.length();

		for (int start = 0; start < end; ) {
			while (start < end && Character.isWhitespace(_name.charAt(start)))
				start++;

			int wordEnd = start;
			while (wordEnd < end && !Character.isWhitespace(_name.charAt(wordEnd)))
				wordEnd++;

			if (setCoordinates(start, wordEnd))
				return;

			start = wordEnd;
		}

		throw new IllegalArgumentException("Name '" + _name + "' does not have :X:Y coordinates");
	}

	/** Looks in a word for the last ":X:Y" (two numbers), which can be followed by something that is not a number. */
	private boolean setCoordinates(int start, int wordEnd) {
		for (int colon = wordEnd - 1; colon >= start; colon--) {
			if (_name.charAt(colon) != ':')
				continue;

			int xStart = colon + 1;
			int xEnd = endOfNumber(xStart, wordEnd);
			if (xEnd == xStart || xEnd >= wordEnd || _name.charAt(xEnd) != ':')
				continue;

			int yStart = xEnd + 1;
			int yEnd = endOfNumber(yStart, wordEnd);
			// At most 9 digits, so that they fit in an int
			if (yEnd == yStart || xEnd - xStart > 9 || yEnd - yStart > 9)
				continue;

			_x = Integer.parseInt(_name.substring(xStart, xEnd));
			_y = Integer.parseInt(_name.substring(yStart, yEnd));
			_baseStart = start;
			_baseEnd = colon;
			_coordinatesOf = _name;
			return true;
		}

		return false;
	}

	private int endOfNumber(int from, int to) {
		int i = from;
		while (i < to && _name.charAt(i) >= '0' && _name.charAt(i) <= '9')
			i++;
		return i;
	}

	/**
	 * Whether the reads can be optical duplicates: they are in the same tile (the same name before the coordinates)
	 * and the euclidean distance between them, truncated to an integer, is at most the one of -d. It is always true
	 * if -d was not given.
	 */
	public boolean isOpticallyClose(Sequence seq2) {
		long dist = Configuration.OPTICAL_DIST;

		if (dist <= 0)
			return true;

		setCoordinates();
		seq2.setCoordinates();

		long dx = _x - seq2._x;
		long dy = _y - seq2._y;

		// floor(sqrt(dx*dx + dy*dy)) <= dist
		if (dx * dx + dy * dy >= (dist + 1) * (dist + 1))
			return false;

		int length = _baseEnd - _baseStart;

		return length == seq2._baseEnd - seq2._baseStart && _name.regionMatches(_baseStart, seq2._name, seq2._baseStart, length);
	}

	public boolean isFirstSeq() {
		return _isFirstSeq;
	}

	public void setQuals(String quals) {
		_quals = quals;
		_avgQual = -1;
	}

	public float getAvgQual() {
		if ((_avgQual < 0) && (_quals != null)) {
			_avgQual = 0;

			for (int i = 0; i < _quals.length(); i++)
				_avgQual += (int) _quals.charAt(i);

			_avgQual /= (float) _quals.length();
		}

		return _avgQual;
	}

	/**
	 * Whether this read is preferred over a duplicate: the highest average quality wins and, if they
	 * are exactly equal, the lowest name, so the kept read does not depend on the arrival order.
	 */
	public boolean isBetterThan(Sequence s) {
		float qual = getAvgQual();
		float qual2 = s.getAvgQual();

		if (qual != qual2)
			return qual > qual2;

		return hasLowerNameThan(s);
	}

	boolean hasLowerNameThan(Sequence s) {
		return _name != null && s._name != null && _name.compareTo(s._name) < 0;
	}

	public int getDist() {
		return _dist;
	}

	public int setDist(Sequence seq2) {

		if (_length != seq2._length) {
			_dist = _length;
			return _dist;
		}

		_dist = 0;
		int popc, i;

		for (i = 0; i < _compressSuffixLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			_dist += popc >> 1;

		if ((popc % 2) > 0)
			_dist++;
		}

		return _dist;
	}

	public long getKey() {
		return _key;
	}

	public void setKey(String bases, short prefixLength) {
		_key = 0;

		for (int i = 0; i < prefixLength; i++) {
			_key *= 5;
			_key += Sequence.codePrefix(bases.charAt(i));
		}
	}

	public void setSuffix(String bases, int prefixLength, int numBasesCompared) {

		int i,j;
		int iterBases = prefixLength;

		_compressSuffix = null;
		_compressSuffixRemainder = null;

		// Set sequence length
		_length = bases.length();

		// Layout of the compressed suffix
		int totalSuffixLength = _length - prefixLength;
		int suffixLength = Sequence.getSuffixLengthToCompare(_length, prefixLength, totalSuffixLength, numBasesCompared, !_isFirstSeq);
		int suffixLengthDiv = suffixLength >> 4;
		int suffixLengthMod = suffixLength & 15;
		int suffixLengthRemainder = totalSuffixLength - suffixLength;
		int suffixLengthRemainderDiv = suffixLengthRemainder >> 4;
		int suffixLengthRemainderMod = suffixLengthRemainder & 15;

		// Code the part of the suffix to be compared (if any)
		if (suffixLength > 0) {
			_compressSuffixLength = (suffixLengthMod > 0)? suffixLengthDiv + 1 : suffixLengthDiv;
			_compressSuffix = new long[_compressSuffixLength];

			for (i = 0; i < suffixLengthDiv; i++) {
				_compressSuffix[i] = 0;

				for (j = 0; j < 16; j++) {
					_compressSuffix[i] <<= 4;
					_compressSuffix[i] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}

			if (suffixLengthMod > 0) {
				_compressSuffix[_compressSuffix.length - 1] = 0;

				for (i = 0; i < suffixLengthMod; i++) {
					_compressSuffix[_compressSuffix.length - 1] <<= 4;
					_compressSuffix[_compressSuffix.length - 1] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}
		}

		// Code the remainder of the suffix (if any)
		if (suffixLengthRemainder > 0) {
			_compressSuffixRemainder = new long[(suffixLengthRemainderMod > 0)? suffixLengthRemainderDiv + 1 : suffixLengthRemainderDiv];

			for (i = 0; i < suffixLengthRemainderDiv; i++) {
				_compressSuffixRemainder[i] = 0;

				for (j = 0; j < 16; j++) {
					_compressSuffixRemainder[i] <<= 4;
					_compressSuffixRemainder[i] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}

			if (suffixLengthRemainderMod > 0) {
				_compressSuffixRemainder[_compressSuffixRemainder.length - 1] = 0;

				for (i = 0; i < suffixLengthRemainderMod; i++) {
					_compressSuffixRemainder[_compressSuffixRemainder.length - 1] <<= 4;
					_compressSuffixRemainder[_compressSuffixRemainder.length - 1] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}
		}
	}

	public boolean equal(Sequence seq2, short misMatch) {

		int numMisMatch = 0;
		int popc, i;

		for (i = 0; i < _compressSuffixLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			numMisMatch += popc >> 1;

		if ((popc % 2) > 0)
			numMisMatch++;

		if (numMisMatch > misMatch)
			return false;
		}

		return true;
	}

	/**
	 * Adds the mismatches with seq2 to numMisMatch, stopping as soon as the total exceeds misMatch.
	 * The sequences are equal if the returned total is not greater than misMatch.
	 */
	public int misMatches(Sequence seq2, short misMatch, int numMisMatch) {

		int popc, i;

		for (i = 0; i < _compressSuffixLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			numMisMatch += popc >> 1;

		if ((popc % 2) > 0)
			numMisMatch++;

		if (numMisMatch > misMatch)
			break;
		}

		return numMisMatch;
	}

	private char[] decodeBases(short prefixLength, int numBasesCompared) {

		char[] bases = new char[_length];

		// Decode the prefix
		int iterBases = prefixLength - 1;
		long auxKey = _key;
		int i,j;

		for (i = 0; i < prefixLength; i++) {
			bases[iterBases] = (char) Sequence.decodePrefixTab[(int) (auxKey % 5)];
			auxKey *= 0.2;
			iterBases--;
		}

		int totalSuffixLength = _length - prefixLength;
		int suffixLengthToCompare = Sequence.getSuffixLengthToCompare(_length, prefixLength, totalSuffixLength, numBasesCompared, !_isFirstSeq);
		int suffixLengthRemainder = totalSuffixLength - suffixLengthToCompare;

		// Decode the suffix
		if (suffixLengthToCompare > 0) {
			int suffixLengthToCompareMod = suffixLengthToCompare & 15;
			iterBases = (prefixLength + suffixLengthToCompare) - 1;
			int iterSuffix = _compressSuffix.length - 1;
			long auxCompress = _compressSuffix[iterSuffix];
			int numDecoded = 0;

			if (suffixLengthToCompareMod > 0) {
				for (i = 0; i < suffixLengthToCompareMod; i++) {
					bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
				numDecoded++;
			}

			for (; numDecoded < _compressSuffix.length; numDecoded++) {
				auxCompress = _compressSuffix[iterSuffix];

				for (j = 0; j < 16; j++) {
					bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
			}
		}

		// Decode the remainder of the suffix
		if (suffixLengthRemainder > 0) {
			int suffixLengthRemainderMod = suffixLengthRemainder & 15;
			iterBases = _length - 1;
			int iterSuffix = _compressSuffixRemainder.length - 1;
			long auxCompress = _compressSuffixRemainder[iterSuffix];
			int numDecoded = 0;

			if (suffixLengthRemainderMod > 0) {
				for (i = 0; i < suffixLengthRemainderMod; i++) {
					bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
				numDecoded++;
			}

			for (; numDecoded < _compressSuffixRemainder.length; numDecoded++) {
				auxCompress = _compressSuffixRemainder[iterSuffix];

				for (j = 0; j < 16; j++) {
					bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
			}
		}

		return bases;
	}

	@Override
	public String toString() {

		StringWriter stringWriter = new StringWriter();

		try {
			// Decode the bases
			char[] bases = decodeBases((_isFirstSeq)? Configuration.PREFIX_LENGTH : 0, Configuration.NUMBER_BASES_COMPARED);

			// Print name and bases
			stringWriter.write(_name);
			stringWriter.write(LF);
			stringWriter.write(bases);

			// Print quality scores if available
			if (_quals != null) {
				stringWriter.write(COMMENT_LINE);
				stringWriter.write(_quals);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return stringWriter.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(_key);
		WritableUtils.writeVInt(out, _length);
		out.writeUTF(_name);
		out.writeBoolean(_isFirstSeq);

		if (_compressSuffixLength > 0) {
			WritableUtils.writeVInt(out, _compressSuffixLength);

			for (int i = 0; i < _compressSuffixLength; i++)
				out.writeLong(_compressSuffix[i]);
		} else {
			out.writeByte(0);
		}

		if (_compressSuffixRemainder != null) {
			WritableUtils.writeVInt(out, _compressSuffixRemainder.length);

			for (int i = 0; i < _compressSuffixRemainder.length; i++)
				out.writeLong(_compressSuffixRemainder[i]);
		} else {
			out.writeByte(0);
		}

		if (_quals != null)
			out.writeUTF(_quals);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_key = in.readLong();
		_length = WritableUtils.readVInt(in);
		_name = in.readUTF();
		// The coordinates were checked by the mapper: they are taken when a comparison needs them
		_isFirstSeq = in.readBoolean();
		_compressSuffixLength = WritableUtils.readVInt(in);
		_compressSuffix = null;
		_compressSuffixRemainder = null;
		_quals = null;
		_avgQual = -1;

		if (_compressSuffixLength > 0) {
			_compressSuffix = new long[_compressSuffixLength];

			for (int i = 0; i < _compressSuffixLength; i++)
				_compressSuffix[i] = in.readLong();
		}

		int compressSuffixRemainderLength = WritableUtils.readVInt(in);

		if (compressSuffixRemainderLength > 0) {
			_compressSuffixRemainder = new long[compressSuffixRemainderLength];

			for (int i = 0; i < compressSuffixRemainderLength; i++)
				_compressSuffixRemainder[i] = in.readLong();			
		}

		if (Configuration.FILE_FORMAT == FileFormat.FILE_FORMAT_FASTQ) {
			try {
				_quals = in.readUTF();
			} catch (EOFException eof) {
				_quals = null;
			}
		}
	}

	private static final int getSuffixLengthToCompare(int length, int prefixLength, int totalSuffixLength, int numBasesCompared, boolean isSecondPairedRead) {

		if (isSecondPairedRead && numBasesCompared > 0) {
			numBasesCompared = numBasesCompared - length;

			if (numBasesCompared <= 0)
				return 0;

			return numBasesCompared;
		}

		if (numBasesCompared > length)
			numBasesCompared = length;

		return (numBasesCompared < 0)? totalSuffixLength : numBasesCompared - prefixLength;
	}
}
