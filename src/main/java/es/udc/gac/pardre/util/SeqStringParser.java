package es.udc.gac.pardre.util;

import java.util.StringTokenizer;

public class SeqStringParser {

	private short _prefixLength;
	private char _minQual;

	public SeqStringParser(short prefixLength, char minQual) {
		this._prefixLength = prefixLength;
		this._minQual = minQual;
	}

	public int getSeq(String s, Sequence seq) {
		if (s.equals("")) {
			System.out.println("Found empty string " + s);
			return 0;
		}

		StringTokenizer st = new StringTokenizer(s, "\n");
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no name available");
		String str = st.nextToken();
		String strQuals;

		// Read the sequence name
		// Everything but the @
		seq.set_name(str.substring(1));

		// Read the sequence bases
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");
		str = st.nextToken();

		// Read the comment line
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no comment available");
		strQuals = st.nextToken();

		// Read the qualities
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no qualities available");
		strQuals = st.nextToken();

		seq.set_length(str.length());
		seq.set_prefix_length(_prefixLength);
		seq.set_quals(strQuals);
		seq.setKey(str, _minQual);
		seq.setSuffix(str, _minQual);

		return seq.get_length();
	}
}
