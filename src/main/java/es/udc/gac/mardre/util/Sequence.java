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

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

public class Sequence implements Writable {

	private static final StringWriter stringWriter = new StringWriter();

	private static final short[] decodePrefixTab = { 'A', 'C', 'G', 'T', 'N' };

	private static final short[] codePrefixTab = { 0, 4, 1, 4, 4, 4, 2, // A -> G
			4, 4, 4, 4, 4, 4, 4, // H->N
			4, 4, 4, 4, 4, 3, 4, // O->U
			4, 4, 4, 4, 4 // V->Z
	};

	private static final short[] decodeTab = { 'N', 'A', 'C', 'N', 'G', 'N', 'N', 'N', 'T' };

	private static final short[] codeTab = { 1, 0, 2, 0, 0, 0, 4, // A -> G
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
	private boolean _isFirstSeq;
	private String _name;
	private String _quals;
	private float _avgQual;

	public Sequence() {
		_name = null;
		_key = 0;
		_compressSuffix = null;
		_length = -1;
		_isFirstSeq = false;
		_quals = null;
		_avgQual = -1;
		_dist = -1;
	}

	public Sequence(Sequence s) {
		_name = s._name;
		_key = s._key;
		_compressSuffix = s._compressSuffix;
		_length = s._length;
		_isFirstSeq = s._isFirstSeq;
		_quals = s._quals;
		_avgQual = s._avgQual;
		_dist = s._dist;
	}

	public void setName(String name) {
		_name = name;
	}

	public String getName() {
		return _name;
	}

	public void setLength(int length) {
		_length = (short) length;
	}

	public int getLength() {
		return _length;
	}

	public void setIsFirstSeq(boolean val) {
		_isFirstSeq = val;
	}

	public boolean isFirstSeq() {
		return _isFirstSeq;
	}

	public String getQuals() {
		return _quals;
	}

	public void setQuals(String quals) {
		_quals = quals;
	}

	public float getAvgQual() {
		if ((_avgQual == -1) && (_quals != null)) {
			_avgQual = 0;
			int length = _quals.length();

			for (int i = 0; i < length; i++) {
				_avgQual += _quals.charAt(i);
			}
			_avgQual /= (float) length;
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
		int popc;
		long iter1, iter2;

		for (int i = 0; i < _compressSuffix.length; i++) {
			iter1 = _compressSuffix[i];
			iter2 = seq2._compressSuffix[i];
			popc = Long.bitCount(iter1 ^ iter2);
			_dist += popc / 2;

			if ((popc % 2) > 0)
				_dist++;
		}

		return _dist;
	}

	public long getKey() {
		return _key;
	}

	public long setKey(String bases, short prefixLength) {
		_key = 0;

		for (int i = 0; i < prefixLength; i++) {
			_key *= 5;
			_key += Sequence.codePrefix(bases.charAt(i));
		}

		return _key;
	}

	public long[] getSufix() {
		return _compressSuffix;
	}

	public void setSuffix(String bases, short prefixLength) {
		// 16 bases per element
		int suffixLength = _length - prefixLength;
		int compressLength = (short) (suffixLength / 16);
		int iterBases = prefixLength;

		if ((suffixLength % 16) > 0)
			compressLength++;

		_compressSuffix = new long[compressLength];

		for (int i = 0; i < suffixLength / 16; i++) {
			_compressSuffix[i] = 0;

			for (int j = 0; j < 16; j++) {
				_compressSuffix[i] <<= 4;
				_compressSuffix[i] += Sequence.code(bases.charAt(iterBases));
				iterBases++;
			}
		}

		if ((suffixLength % 16) > 0) {
			_compressSuffix[compressLength - 1] = 0;

			for (int i = 0; i < suffixLength % 16; i++) {
				_compressSuffix[compressLength - 1] <<= 4;
				_compressSuffix[compressLength - 1] += Sequence.code(bases.charAt(iterBases));
				iterBases++;
			}
		}
	}

	public boolean equal(Sequence seq2, short missMatch) {

		if (_length != seq2._length)
			return false;

		short numMissMatch = 0;
		int popc;
		long iter1, iter2;

		for (int i = 0; i < _compressSuffix.length; i++) {
			iter1 = _compressSuffix[i];
			iter2 = seq2._compressSuffix[i];
			popc = Long.bitCount(iter1 ^ iter2);
			numMissMatch += popc / 2;

			if ((popc % 2) > 0)
				numMissMatch++;

			if (numMissMatch > missMatch)
				return false;
		}

		return true;
	}

	public boolean equal(Sequence seq2, short missMatch, short[] numMissMatch) {

		if (_length != seq2._length)
			return false;

		int popc;
		long iter1, iter2;

		for (int i = 0; i < _compressSuffix.length; i++) {
			iter1 = _compressSuffix[i];
			iter2 = seq2._compressSuffix[i];
			popc = Long.bitCount(iter1 ^ iter2);
			numMissMatch[0] += popc / 2;

			if ((popc % 2) > 0)
				numMissMatch[0]++;

			if (numMissMatch[0] > missMatch)
				return false;
		}

		return true;
	}

	private void decodeBases(char[] bases) {

		// Decode the prefix
		short prefixLength = Configuration.prefixLength;
		int iterBases = prefixLength - 1;
		long auxKey = _key;

		for (int i = 0; i < prefixLength; i++) {
			bases[iterBases] = (char) Sequence.decodePrefixTab[(int) (auxKey % 5)];
			auxKey /= 5;
			iterBases--;
		}

		// Decode the suffix (16 bases per element)
		iterBases = _length - 1;
		int iterSuffix = _compressSuffix.length - 1;
		long auxCompress = _compressSuffix[iterSuffix];
		int suffixLength = _length - prefixLength;
		int numDecoded = 0;

		if ((suffixLength % 16) > 0) {
			for (int i = 0; i < (suffixLength % 16); i++) {
				bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
			numDecoded++;
		}

		for (; numDecoded < _compressSuffix.length; numDecoded++) {
			auxCompress = _compressSuffix[iterSuffix];

			for (int j = 0; j < 16; j++) {
				bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
		}
	}

	@Override
	public String toString() {
		stringWriter.getBuffer().setLength(0);
		BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);

		try {
			// Print the name
			bufferedWriter.write(_name, 0, _name.length());
			bufferedWriter.newLine();

			char[] bases = new char[_length];
			
			// Decode the bases
			decodeBases(bases);

			// Print the bases
			bufferedWriter.write(bases);

			// Print the quality scores if available
			if (_quals != null) {
				bufferedWriter.newLine();
				bufferedWriter.write('+');
				bufferedWriter.newLine();
				bufferedWriter.write(_quals, 0, _quals.length());
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
		out.writeShort(_compressSuffix.length);

		for (int i = 0; i < _compressSuffix.length; i++)
			out.writeLong(_compressSuffix[i]);

		if (_quals != null)
			out.writeUTF(_quals);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_key = in.readLong();
		_length = in.readShort();
		_name = in.readUTF();
		_isFirstSeq = in.readBoolean();
		int compressLength = in.readShort();
		_compressSuffix = new long[compressLength];

		for (int i = 0; i < compressLength; i++)
			_compressSuffix[i] = in.readLong();

		try {
			_quals = in.readUTF();
		} catch (EOFException ex) {
			_quals = null;
		}
	}
}
