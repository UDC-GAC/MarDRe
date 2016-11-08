package es.udc.gac.pardre.util;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FastQSequence extends Sequence {

	private String _quals;
	private float _avgQual;

	FastQSequence() {
		super();
		_quals = null;
		_avgQual = -1;
	}

	FastQSequence(FastQSequence s) {
		super(s);
		_quals = s.getQuals();
		_avgQual = s.getAvgQual();
	}

	public void setQuals(String quals) {
		if (quals.length() != _length) {
			throw new IllegalArgumentException(
					"The number of qualities should be " + _length + " but are "
							+ quals.length());
		}
		_quals = quals;
	}

	public String getQuals() {
		return _quals;
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
				_key += Sequence.codePrefix(bases.charAt(i));
		}

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

		if (!_quals.isEmpty()) {
			writer.write('+');
			writer.newLine();
			writer.write(_quals, 0, _quals.length());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.write(out);
		out.writeUTF(_quals);
		out.writeFloat(_avgQual);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.readFields(in);
		_quals = in.readUTF();
		_avgQual = in.readFloat();
	}
}