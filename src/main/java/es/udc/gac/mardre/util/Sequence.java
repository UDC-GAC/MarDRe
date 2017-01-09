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

	private String _name;
	private short _length;
	private short _prefixLength;
	private long _key;
	private long[] _compressSuffix;
	private short _compressLength;
	private boolean _isFirstSeq;
	private String _quals;
	private float _avgQual;
	private short _dist;

	public Sequence() {
		_name = null;
		_key = 0;
		_compressSuffix = null;
		_compressLength = 0;
		_length = -1;
		_prefixLength = -1;
		_isFirstSeq = false;
		_quals = null;
		_avgQual = -1;
		_dist = -1;
	}

	public Sequence(Sequence s) {
		_name = s._name;
		_key = s._key;
		_compressSuffix = s._compressSuffix;
		_compressLength = s._compressLength;
		_length = s._length;
		_prefixLength = s._prefixLength;
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

	public void setPrefixLength(short length) {
		_prefixLength = length;
	}

	public short getPrefixLength() {
		return _prefixLength;
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

			for (int i = 0; i < _length; i++) {
				_avgQual += _quals.charAt(i);
			}
			_avgQual /= (float) _length;
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

		for (int i = 0; i < _compressLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			_dist += popc / 2;

			if ((popc % 2) > 0)
				_dist++;
		}

		return _dist;
	}

	public long getKey() {
		return _key;
	}

	public long setKey(String bases) {
		_key = 0;

		for (int i = 0; i < _prefixLength; i++) {
			_key *= 5;
			_key += Sequence.codePrefix(bases.charAt(i));
		}

		return _key;
	}

	public long[] getSufix() {
		return _compressSuffix;
	}

	public void setSuffix(String bases) {
		// 16 bases per element
		int suffixLength = _length - _prefixLength;
		_compressLength = (short) (suffixLength / 16);
		int iterBases = _prefixLength;

		if ((suffixLength % 16) > 0)
			_compressLength++;

		_compressSuffix = new long[_compressLength];

		for (int i = 0; i < suffixLength / 16; i++) {
			_compressSuffix[i] = 0;

			for (int j = 0; j < 16; j++) {
				_compressSuffix[i] <<= 4;
				_compressSuffix[i] += Sequence.code(bases.charAt(iterBases));
				iterBases++;
			}
		}

		if ((suffixLength % 16) > 0) {
			_compressSuffix[_compressLength - 1] = 0;

			for (int i = 0; i < suffixLength % 16; i++) {
				_compressSuffix[_compressLength - 1] <<= 4;
				_compressSuffix[_compressLength - 1] += Sequence.code(bases.charAt(iterBases));
				iterBases++;
			}
		}
	}

	public boolean equal(Sequence seq2, short missMatch) {

		if (_length != seq2._length)
			return false;

		short numMissMatch = 0;
		int popc;

		for (int i = 0; i < _compressLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
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

		for (int i = 0; i < _compressLength; i++) {
			popc = Long.bitCount(_compressSuffix[i] ^ seq2._compressSuffix[i]);
			numMissMatch[0] += popc / 2;

			if ((popc % 2) > 0)
				numMissMatch[0]++;

			if (numMissMatch[0] > missMatch)
				return false;
		}

		return true;
	}

	public void print(BufferedWriter writer) throws IOException {

		// Print the name
		writer.write(_name, 0, _name.length());
		writer.newLine();

		// Decode the bases
		decodeBases(writer);

		// Print the quality scores if available
		if (_quals != null) {
			writer.newLine();
			writer.write('+');
			writer.newLine();
			writer.write(_quals, 0, _quals.length());
		}
	}

	private void decodeBases(BufferedWriter writer) throws IOException {

		// Decode the prefix
		char[] bases = new char[_length];
		int iterBases = _prefixLength - 1;
		long auxKey = _key;

		for (int i = 0; i < _prefixLength; i++) {
			bases[iterBases] = (char) Sequence.decodePrefixTab[(int) (auxKey % 5)];
			auxKey /= 5;
			iterBases--;
		}

		// Decode the suffix
		// 16 bases per element
		iterBases = _length - 1;
		long iterSuffix = _compressSuffix[_compressLength - 1];
		int suffixLength = _length - _prefixLength;
		int numDecoded = 0;

		if ((suffixLength % 16) > 0) {

			for (int i = 0; i < (suffixLength % 16); i++) {
				bases[iterBases] = (char) Sequence.decodeTab[(int) (iterSuffix & 15)];
				iterSuffix >>= 4;
			iterBases--;
			}
			iterSuffix--;
			numDecoded++;
		}

		for (; numDecoded < _compressLength; numDecoded++) {
			for (int j = 0; j < 16; j++) {
				bases[iterBases] = (char) Sequence.decodeTab[(int) (iterSuffix & 15)];
				iterSuffix >>= 4;
			iterBases--;
			}
			iterSuffix--;
		}

		// Print the bases
		writer.write(bases);
	}

	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		BufferedWriter bw = new BufferedWriter(sw);

		try {
			print(bw);
			bw.close();
			sw.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return sw.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(_key);
		out.writeShort(_length);
		out.writeShort(_prefixLength);
		out.writeUTF(_name);
		out.writeBoolean(_isFirstSeq);
		out.writeShort(_compressLength);

		for (int i = 0; i < _compressLength; i++)
			out.writeLong(_compressSuffix[i]);

		if (_quals != null) {
			out.writeUTF(_quals);
			//out.writeFloat(_avgQual);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_key = in.readLong();
		_length = in.readShort();
		_prefixLength = in.readShort();
		_name = in.readUTF();
		_isFirstSeq = in.readBoolean();
		_compressLength = in.readShort();
		_compressSuffix = new long[_compressLength];

		for (int i = 0; i < _compressLength; i++)
			_compressSuffix[i] = in.readLong();

		try {
			_quals = in.readUTF();
		} catch (EOFException ex) {
			_quals = null;
		}

		//if (_quals != null)
		//_avgQual = in.readFloat();
	}
}
