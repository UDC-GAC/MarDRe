package es.udc.gac.pardre.util;

import java.util.StringTokenizer;

public class FastQSeqStringParser extends SeqStringParser {

	public FastQSeqStringParser(short prefixLength, char minQual) {
		super(prefixLength, minQual);
	}

	@Override
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

		// Read the sequence name
		// Everything but the '@'
		seq.setName(str.substring(1));

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
		String strQuals = st.nextToken();

		// Read the qualities
		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": no qualities available");
		strQuals = st.nextToken();

		seq.setLength(str.length());
		seq.setPrefixLength(this.get_prefixLength());
		seq.setQuals(strQuals);
		seq.setKey(str, this.get_minQual());
		seq.setSuffix(str, this.get_minQual());

		return seq.getLength();
	}
}