package es.udc.gac.mardre.util;

public abstract class SeqStringParser {

	private short _prefixLength;
	private char _minQual;

	public SeqStringParser(short prefixLength, char minQual) {
		this._prefixLength = prefixLength;
		this._minQual = minQual;
	}

	public short get_prefixLength() {
		return _prefixLength;
	}

	public void set_prefixLength(short _prefixLength) {
		this._prefixLength = _prefixLength;
	}

	public char get_minQual() {
		return _minQual;
	}

	public void set_minQual(char _minQual) {
		this._minQual = _minQual;
	}

	public abstract int getSeq(String s, Sequence seq);
}