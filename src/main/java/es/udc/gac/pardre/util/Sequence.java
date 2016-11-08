package es.udc.gac.pardre.util;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

public abstract class Sequence implements Writable {

	protected static final short[] decodePrefixTab = { 'A', 'C', 'G', 'T', 'N' };

	protected static final  short[] codePrefixTab = { 0, 4, 1, 4, 4, 4, 2, // A -> G
			4, 4, 4, 4, 4, 4, 4, // H->N
			4, 4, 4, 4, 4, 3, 4, // O->U
			4, 4, 4, 4, 4 // V->Z
	};

	protected static final short[] decodeTab = { 'N', 'A', 'C', 'N', 'G', 'N', 'N',
			'N', 'T' };

	protected static final short[] codeTab = { 1, 0, 2, 0, 0, 0, 4, // A -> G
			0, 0, 0, 0, 0, 0, 0, // H->N
			0, 0, 0, 0, 0, 8, 0, // O->U
			0, 0, 0, 0, 0 // V->Z
	};

	protected static short codePrefix(char ch) {
		short aux = (short) ch;

		if ((ch >= 'A') && (ch <= 'Z')) {
			aux -= 'A';
		} else if ((ch >= 'a') && (ch <= 'z')) {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected chararter " + ch
					+ " in the bases of fastq file");
		}

		return Sequence.codePrefixTab[aux];
	}

	protected static short code(char ch) {
		short aux = (short) ch;

		if ((ch >= 'A') && (ch <= 'Z')) {
			aux -= 'A';
		} else if ((ch >= 'a') && (ch <= 'z')) {
			aux -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected chararter " + ch
					+ " in the bases of fastq file");
		}

		return Sequence.codeTab[aux];
	}

	protected String _name;
	protected int _length;
	protected short _prefixLength;
	protected long _key;
	protected long[] _compressSuffix;
	protected boolean _isFirstSeq;

	Sequence() {
		_name = null;
		_key = 0;
		_compressSuffix = null;
		_length = -1;
		_prefixLength = -1;
		_isFirstSeq = false;
	}

	Sequence(Sequence s) {
		_name = s._name;
		_key = s._key;
		_compressSuffix = s._compressSuffix;
		_length = s._length;
		_prefixLength = s._prefixLength;
		_isFirstSeq = s._isFirstSeq;
	}

	public void setName(String name) {
		_name = name;
	}

	public String getName() {
		return _name;
	}

	public void setLength(int length) {
		_length = length;
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

	public long getKey() {
		return _key;
	}

	public long[] getSufix() {
		return _compressSuffix;
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
		out.writeInt(_length);
		out.writeShort(_prefixLength);
		out.writeUTF(_name);
		out.writeBoolean(_isFirstSeq);
		out.writeInt(_compressSuffix.length);

		for (int i = 0; i < _compressSuffix.length; i++)
			out.writeLong(_compressSuffix[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_key = in.readLong();
		_length = in.readInt();
		_prefixLength = in.readShort();
		_name = in.readUTF();
		_isFirstSeq = in.readBoolean();
		int compressSuffixLength = in.readInt();
		_compressSuffix = new long[compressSuffixLength];

		for (int i = 0; i < compressSuffixLength; i++)
			_compressSuffix[i] = in.readLong();
	}

	public String toStringFull() {
		return toString() + ", isFirstSeq=" + _isFirstSeq + ", compressSuffix.length=" + _compressSuffix.length
				+ ", prefixLength=" + _prefixLength;
	}

	public abstract long setKey(String bases, char minQual);

	public abstract void setSuffix(String bases, char minQual);

	public abstract void print(BufferedWriter writer) throws IOException;
}