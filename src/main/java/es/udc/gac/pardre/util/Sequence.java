package es.udc.gac.pardre.util;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

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
			ch -= 'A';
		} else if (ch >= 'a' && ch <= 'z') {
			ch -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected character " + ch 
					+ " in the bases");
		}
		return Sequence.codePrefixTab[aux];
	}

	private static short code(char ch) {
		short aux = (short) ch;

		if (ch >= 'A' && ch <= 'Z') {
			ch -= 'A';
		} else if (ch >= 'a' && ch <= 'z') {
			ch -= 'a';
		} else {
			throw new IllegalArgumentException("Unexpected character " + ch 
					+ " in the bases");
		}
		return Sequence.codeTab[aux];
	}

	private String _name;
	private int _length;
	private short _prefixLength;
	private long _key;
	private long[] _compressSuffix;
	private boolean _isFirstSeq;
	private String _quals;
	private float _avgQual;
	private FileFormat _format;

	public Sequence() {
		_name = null;
		_key = 0;
		_compressSuffix = null;
		_length = -1;
		_prefixLength = -1;
		_isFirstSeq = false;
		_quals = null;
		_avgQual = -1;
		_format = FileFormat.FILE_FORMAT_UNKNOWN;
	}

	public Sequence(Sequence s) {
		_name = s._name;
		_key = s._key;
		_compressSuffix = s._compressSuffix;
		_length = s._length;
		_prefixLength = s._prefixLength;
		_isFirstSeq = s._isFirstSeq;
		_quals = s._quals;
		_avgQual = s._avgQual;
		_format = s._format;
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

	public String getQuals() {
		return _quals;
	}

	public void setQuals(String quals) {
		_quals = quals;
	}

	public void releaseQuals() {
		_quals = null;
	}

	public void setAvgQual(float avgQual) {
		_avgQual = avgQual;
	}

	public float getAvgQual() {
		if ((_avgQual == -1) && (!_quals.isEmpty())) {
			_avgQual = 0;

			for (int i = 0; i < _length; i++) {
				_avgQual += _quals.charAt(i);
			}
			_avgQual /= (float) _length;
		}

		return _avgQual;
	}

	public FileFormat getFormat() {
		return _format;
	}

	public void setFormat(FileFormat _format) {
		this._format = _format;
	}

	public long getKey() {
		return _key;
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

		if (_format == FileFormat.FILE_FORMAT_FASTA) {
			for (int i = 0; i < limit; i++) {
				_key *= 5;
				_key += Sequence.codePrefix(bases.charAt(i));
			}
		} else {
			for (int i = 0; i < limit; i++) {
				_key *= 5;
				if (_quals.charAt(i) < minQual)
					_key += 4;
				else
					_key += Sequence.codePrefix(bases.charAt(i));
			}
		}

		return _key;
	}

	public long[] getSufix() {
		return _compressSuffix;
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

				if (_format == FileFormat.FILE_FORMAT_FASTA || _quals.charAt(iterBases) >= minQual)
					_compressSuffix[i] += Sequence.code(bases.charAt(iterBases));

				iterBases++;
			}
		}

		if ((suffixLength % 16) > 0) {
			_compressSuffix[compressLength - 1] = 0;

			for (int i = 0; i < suffixLength % 16; i++) {
				_compressSuffix[compressLength - 1] <<= 4;

				if (_format == FileFormat.FILE_FORMAT_FASTA || _quals.charAt(iterBases) >= minQual)
					_compressSuffix[compressLength - 1] += Sequence.code(bases.charAt(iterBases));

				iterBases++;
			}
		}
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

		if (_format == FileFormat.FILE_FORMAT_FASTA)
			writer.write(">");
		else
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
			bases[iterBases] = (char) Sequence.decodePrefixTab[(int) (auxKey % 5)];
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
				bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
			numDecoded++;
		}

		for (; numDecoded < compressLength; numDecoded++) {
			auxCompress = _compressSuffix[iterSuffix];

			for (int j = 0; j < 16; j++) {
				bases[iterBases] = (char) Sequence.decodeTab[(int) (auxCompress & 15)];
				auxCompress >>= 4;
			iterBases--;
			}
			iterSuffix--;
		}

		writer.write(bases);
		writer.newLine();

		if (_format == FileFormat.FILE_FORMAT_FASTQ && !_quals.isEmpty()) {
			writer.write('+');
			writer.newLine();
			writer.write(_quals, 0, _quals.length());
		}
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

		out.writeInt(_format.ordinal());

		if (_format == FileFormat.FILE_FORMAT_FASTQ) {
			out.writeUTF(_quals);
			out.writeFloat(_avgQual);
		}
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

		_format = FileFormat.values()[in.readInt()];

		if (_format == FileFormat.FILE_FORMAT_FASTQ) {
			_quals = in.readUTF();
			_avgQual = in.readFloat();
		}
	}

	public String toStringFull() {
		return toString() + ", isFirstSeq=" + _isFirstSeq + ", compressSuffix.length=" + _compressSuffix.length
				+ ", prefixLength=" + _prefixLength;
	}
}