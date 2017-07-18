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

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class Sequence implements Writable {

	private static final StringWriter stringWriter = new StringWriter();
	private static char[] bases = null;
	private static short compressSuffixLength = -1;
	private static short compressSuffixLengthRemainder = -1;
	private static int totalSuffixLength = -1;
	private static int suffixLength = -1;
	private static int suffixLengthDiv = -1;
	private static int suffixLengthMod = -1;
	private static int suffixLengthRemainder = -1;
	private static int suffixLengthRemainderDiv = -1;
	private static int suffixLengthRemainderMod = -1;

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
	private short _length;
	private short _dist;
	private long[] _compressSuffix;
	private short _compressSuffixLength;
	private long[] _compressSuffixRemainder;
	private boolean _isFirstSeq;
	private String _name;
	private String _quals;
	private float _avgQual;

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
	}

	public void setName(String name) {
		_name = name;
	}

	public void setIsFirstSeq(boolean val) {
		_isFirstSeq = val;
	}

	public boolean isFirstSeq() {
		return _isFirstSeq;
	}

	public void setQuals(String quals) {
		_quals = quals;
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

	public short getDist() {
		return _dist;
	}

	public short setDist(Sequence seq2) {

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

	public void setSuffix(String bases, int prefixLength, int numBasesCompared, boolean isSecondPairedRead) {

		int i,j;
		int iterBases = prefixLength;

		_compressSuffix = null;
		_compressSuffixRemainder = null;

		// Set sequence length
		_length = (short) bases.length();

		int totalSuffixLength = _length - prefixLength;

		// Set compress suffix length
		if (Sequence.totalSuffixLength != totalSuffixLength)
			Sequence.setCompressSuffixLength(_length, prefixLength, numBasesCompared, isSecondPairedRead);

		// Code the part of the suffix to be compared (if any)
		if (Sequence.suffixLength > 0) {
			_compressSuffixLength = Sequence.compressSuffixLength;
			_compressSuffix = new long[_compressSuffixLength];

			for (i = 0; i < Sequence.suffixLengthDiv; i++) {
				_compressSuffix[i] = 0;

				for (j = 0; j < 16; j++) {
					_compressSuffix[i] <<= 4;
					_compressSuffix[i] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}

			if (Sequence.suffixLengthMod > 0) {
				_compressSuffix[_compressSuffix.length - 1] = 0;

				for (i = 0; i < Sequence.suffixLengthMod; i++) {
					_compressSuffix[_compressSuffix.length - 1] <<= 4;
					_compressSuffix[_compressSuffix.length - 1] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}
		}

		// Code the remainder of the suffix (if any)
		if (Sequence.suffixLengthRemainder > 0) {
			_compressSuffixRemainder = new long[Sequence.compressSuffixLengthRemainder];

			for (i = 0; i < Sequence.suffixLengthRemainderDiv; i++) {
				_compressSuffixRemainder[i] = 0;

				for (j = 0; j < 16; j++) {
					_compressSuffixRemainder[i] <<= 4;
					_compressSuffixRemainder[i] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}

			if (Sequence.suffixLengthRemainderMod > 0) {
				_compressSuffixRemainder[_compressSuffixRemainder.length - 1] = 0;

				for (i = 0; i < Sequence.suffixLengthRemainderMod; i++) {
					_compressSuffixRemainder[_compressSuffixRemainder.length - 1] <<= 4;
					_compressSuffixRemainder[_compressSuffixRemainder.length - 1] += Sequence.code(bases.charAt(iterBases));
					iterBases++;
				}
			}
		}
	}

	public boolean equal(Sequence seq2, short misMatch) {

		short numMisMatch = 0;
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

	public boolean equal(Sequence seq2, short misMatch, short[] numMisMatch) {

		int popc, i;

		for (i = 0; i < _compressSuffixLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			numMisMatch[0] += popc >> 1;

		if ((popc % 2) > 0)
			numMisMatch[0]++;

		if (numMisMatch[0] > misMatch)
			return false;
		}

		return true;
	}

	private void decodeBases(short prefixLength, int numBasesCompared, boolean isSecondPairedRead) {

		if (Sequence.bases == null || Sequence.bases.length != _length)
			Sequence.bases = new char[_length];

		// Decode the prefix
		int iterBases = prefixLength - 1;
		long auxKey = _key;
		int i,j;

		for (i = 0; i < prefixLength; i++) {
			Sequence.bases[iterBases] = (char) Sequence.decodePrefixTab[(int) (auxKey % 5)];
			auxKey *= 0.2;
			iterBases--;
		}

		int totalSuffixLength = _length - prefixLength;
		int suffixLengthToCompare = Sequence.getSuffixLengthToCompare(_length, prefixLength, totalSuffixLength, numBasesCompared, isSecondPairedRead);
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
					Sequence.bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
				numDecoded++;
			}

			for (; numDecoded < _compressSuffix.length; numDecoded++) {
				auxCompress = _compressSuffix[iterSuffix];

				for (j = 0; j < 16; j++) {
					Sequence.bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
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
					Sequence.bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
				numDecoded++;
			}

			for (; numDecoded < _compressSuffixRemainder.length; numDecoded++) {
				auxCompress = _compressSuffixRemainder[iterSuffix];

				for (j = 0; j < 16; j++) {
					Sequence.bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
					auxCompress >>= 4;
				iterBases--;
				}
				iterSuffix--;
			}
		}
	}

	@Override
	public String toString() {

		stringWriter.getBuffer().setLength(0);
		BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);

		try {
			// Decode the bases
			decodeBases((_isFirstSeq)? Configuration.PREFIX_LENGTH : 0, Configuration.NUMBER_BASES_COMPARED, !_isFirstSeq);

			// Print name and bases
			bufferedWriter.write(_name);
			bufferedWriter.newLine();
			bufferedWriter.write(Sequence.bases);

			// Print quality scores if available
			if (_quals != null) {
				bufferedWriter.newLine();
				bufferedWriter.write('+');
				bufferedWriter.newLine();
				bufferedWriter.write(_quals);
			}

			bufferedWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return stringWriter.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(_key);
		out.writeShort(_length);
		out.writeUTF(_name);
		out.writeBoolean(_isFirstSeq);

		if (_compressSuffixLength > 0) {
			out.writeByte(_compressSuffixLength);

			for (int i = 0; i < _compressSuffixLength; i++)
				out.writeLong(_compressSuffix[i]);
		} else {
			out.writeByte(0);
		}

		if (_compressSuffixRemainder != null) {
			out.writeByte(_compressSuffixRemainder.length);

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
		_length = in.readShort();
		_name = in.readUTF();
		_isFirstSeq = in.readBoolean();
		_compressSuffixLength = in.readByte();
		_compressSuffix = null;
		_compressSuffixRemainder = null;
		_quals = null;

		if (_compressSuffixLength > 0) {
			_compressSuffix = new long[_compressSuffixLength];

			for (int i = 0; i < _compressSuffixLength; i++)
				_compressSuffix[i] = in.readLong();
		}

		int compressSuffixRemainderLength = in.readByte();

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

	private static final void setCompressSuffixLength(int length, int prefixLength, int numBasesCompared, boolean isSecondPairedRead) {

		Sequence.totalSuffixLength = length - prefixLength;

		int suffixLengthToCompare = Sequence.getSuffixLengthToCompare(length, prefixLength, Sequence.totalSuffixLength, numBasesCompared, isSecondPairedRead);

		// Suffix length (to be compared)
		Sequence.suffixLength = suffixLengthToCompare;

		if (Sequence.suffixLength > 0) {
			Sequence.suffixLengthDiv = Sequence.suffixLength >> 4;
			Sequence.suffixLengthMod = Sequence.suffixLength & 15;
			Sequence.compressSuffixLength = (short)((Sequence.suffixLengthMod > 0)? Sequence.suffixLengthDiv + 1 : Sequence.suffixLengthDiv);
		}

		// Remainder suffix length (not to be compared)
		Sequence.suffixLengthRemainder = Sequence.totalSuffixLength - suffixLengthToCompare;

		if (Sequence.suffixLengthRemainder > 0) {
			Sequence.suffixLengthRemainderDiv = Sequence.suffixLengthRemainder >> 4;
			Sequence.suffixLengthRemainderMod = Sequence.suffixLengthRemainder & 15;
			Sequence.compressSuffixLengthRemainder = (short)((Sequence.suffixLengthRemainderMod > 0)? Sequence.suffixLengthRemainderDiv + 1 : Sequence.suffixLengthRemainderDiv);
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
