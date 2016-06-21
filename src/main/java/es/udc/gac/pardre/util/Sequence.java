package es.udc.gac.pardre.util;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

public class Sequence implements Writable {

	private static short[] _decodePrefixTab = { 'A', 'C', 'G', 'T', 'N' };
	private static short[] _codePrefixTab = { 0, 4, 1, 4, 4, 4, 2, // A -> G
			4, 4, 4, 4, 4, 4, 4, // H->N
			4, 4, 4, 4, 4, 3, 4, // O->U
			4, 4, 4, 4, 4 // V->Z
	};

	private static short[] _decodeTab = { 'N', 'A', 'C', 'N', 'G', 'N', 'N',
			'N', 'T' };

	private static short[] _codeTab = { 1, 0, 2, 0, 0, 0, 4, // A -> G
			0, 0, 0, 0, 0, 0, 0, // H->N
			0, 0, 0, 0, 0, 8, 0, // O->U
			0, 0, 0, 0, 0 // V->Z
	};

	private String _name;
	private int _length;
	private short _prefixLength;
	private long _key;
	private long[] _compressSuffix;
	private String _quals;
	private float _avgQual;

	public Sequence() {
		_name = null;
		_quals = null;
		_key = 0;
		_compressSuffix = null;
		_avgQual = -1;
		_length = -1;
		_prefixLength = -1;
	}

	public Sequence(Sequence s) {
		_name = s._name;
		_quals = s._quals;
		_key = s._key;
		_compressSuffix = s._compressSuffix;
		_avgQual = s._avgQual;
		_length = s._length;
		_prefixLength = s._prefixLength;
	}

	public void set_name(String name) {
		_name = name;
	}

	public void set_quals(String quals) {
		if (quals.length() != _length) {
			throw new IllegalArgumentException(
					"The number of qualities should be " + _length + " but are "
							+ quals.length());
		}
		_quals = quals;
	}

	public void releaseQuals() {
		_quals = null;
	}

	public void set_length(int length) {
		_length = length;
	}

	public int get_length() {
		return _length;
	}

	public void set_prefix_length(short length) {
		_prefixLength = length;
	}

	public short get_prefix_length() {
		return _prefixLength;
	}

	public String get_name() {
		return _name;
	}

	private short codePrefix(char ch) {
		short aux = (short) ch;

		if ((ch >= 'A') && (ch <= 'Z')) {
			aux -= 'A';
		} else if ((ch >= 'a') && (ch <= 'z')) {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected chararter " + ch
					+ " in the bases of fastq file");
		}

		return _codePrefixTab[aux];
	}

	private short code(char ch) {
		short aux = (short) ch;

		if ((ch >= 'A') && (ch <= 'Z')) {
			aux -= 'A';
		} else if ((ch >= 'a') && (ch <= 'z')) {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected chararter " + ch
					+ " in the bases of fastq file");
		}

		return _codeTab[aux];
	}

	public long setKey(String bases, char minQual) {
		_key = 0;

		int length = bases.length();

		if (_length != length) {
			throw new IllegalArgumentException("The number of bases should be "
					+ _length + " but are " + length);
		}

		int limit = _prefixLength;

		if (_prefixLength > length)
			limit = length;

		for (int i = 0; i < limit; i++) {
			_key *= 5;
			if (_quals.charAt(i) < minQual)
				_key += 4;
			else
				_key += codePrefix(bases.charAt(i));
		}

		return _key;
	}

	public long getKey() {
		return _key;
	}

	public void setSuffix(String bases, char minQual) {
		int limit = _prefixLength;

		if (_prefixLength > _length)
			limit = _length;

		// 16 bases per element
		int suffixLength = _length - limit;
		int compressLength = suffixLength / 16;
		int iterBases = limit;

		if ((suffixLength % 16) > 0)
			compressLength++;

		_compressSuffix = new long[compressLength];


		for (int i = 0; i < suffixLength / 16; i++) {
			_compressSuffix[i] = 0;

			for (int j = 0; j < 16; j++) {
				_compressSuffix[i] <<= 4;

				if (_quals.charAt(iterBases) >= minQual)
					_compressSuffix[i] += code(bases.charAt(iterBases));

				iterBases++;
			}
		}

		if ((suffixLength % 16) > 0) {
			_compressSuffix[compressLength - 1] = 0;

			for (int i = 0; i < suffixLength % 16; i++) {
				_compressSuffix[compressLength - 1] <<= 4;

				if (_quals.charAt(iterBases) >= minQual)
					_compressSuffix[compressLength - 1] += code(bases.charAt(iterBases));

				iterBases++;
			}
		}
	}

	public float get_avgQual() {
		if ((_avgQual == -1) && (!_quals.isEmpty())) {
			_avgQual = 0;

			for (int i = 0; i < _length; i++) {
				_avgQual += _quals.charAt(i);
			}
			_avgQual /= (float) _length;
		}

		return _avgQual;
	}

	// The boolean parameter indicates if we need to compare the prefixes too ->
	// In general it must be false
	public boolean equal(Sequence seq2, short missMatch, boolean compareKey) {

		if (_length != seq2._length)
			return false;

		if (compareKey) {
			// If the prefix is different they are not equal
			if (_key != seq2._key)
				return false;
		}

		short numMissMatch = 0;
		long aux1, aux2, xorRes;
		int popc;

		for (int i = 0; i < _compressSuffix.length; i++) {
			aux1 = _compressSuffix[i];
			aux2 = seq2._compressSuffix[i];
			xorRes = aux1 ^ aux2;
			popc = Long.bitCount(xorRes);
			numMissMatch += popc / 2;

			if ((popc % 2) > 0)
				numMissMatch++;

			if (numMissMatch > missMatch)
				return false;
		}

		return true;
	}

	public void print(BufferedWriter writer) throws IOException {
		writer.write("@");
		writer.write(_name, 0, _name.length());
		writer.newLine();
		int limit = _prefixLength;

		if (_prefixLength > _length)
			limit = _length;

		// Decode the bases
		// First the prefix
		char[] bases = new char[_length];
		int iterBases = limit - 1;
		long auxKey = _key;

		for (int i = 0; i < limit; i++) {
			bases[iterBases] = (char) _decodePrefixTab[(int) (auxKey % 5)];
			auxKey /= 5;
			iterBases--;
		}

		// Now the suffix
		// 16 bases per element
		int suffixLength = _length - limit;
		int compressLength = suffixLength / 16;

		if ((suffixLength % 16) > 0)
			compressLength++;

		iterBases = _length - 1;
		int iterSuffix = compressLength - 1;
		long auxCompress;
		int numDecoded = 0;

		if ((suffixLength % 16) > 0) {
			auxCompress = _compressSuffix[compressLength - 1];

			for (int i = 0; i < (suffixLength % 16); i++) {
				bases[iterBases] = (char) _decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
			numDecoded++;
		}

		for (; numDecoded < compressLength; numDecoded++) {
			auxCompress = _compressSuffix[iterSuffix];

			for (int j = 0; j < 16; j++) {
				bases[iterBases] = (char) _decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
		}

		writer.write(bases);
		writer.newLine();

		if (!_quals.isEmpty()) {
			writer.write('+');
			writer.newLine();
			writer.write(_quals, 0, _quals.length());
		}
	}

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

	public void write(DataOutput out) throws IOException {
		out.writeLong(_key);
		out.writeInt(_length);
		out.writeShort(_prefixLength);
		out.writeUTF(_name);
		out.writeUTF(_quals);
		out.writeFloat(_avgQual);
		out.writeInt(_compressSuffix.length);

		for (int i = 0; i < _compressSuffix.length; i++)
			out.writeLong(_compressSuffix[i]);
	}

	public void readFields(DataInput in) throws IOException {
		_key = in.readLong();
		_length = in.readInt();
		_prefixLength = in.readShort();
		_name = in.readUTF();
		_quals = in.readUTF();
		_avgQual = in.readFloat();
		int compressSuffixLength = in.readInt();
		_compressSuffix = new long[compressSuffixLength];

		for (int i = 0; i < compressSuffixLength; i++)
			_compressSuffix[i] = in.readLong();
	}

	public String toStringFull() {
		return toString() + ", _compressSuffix.length=" + _compressSuffix.length
				+ ", prefixLength=" + _prefixLength;
	}
}