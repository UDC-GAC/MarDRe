package es.udc.gac.pardre.util;

import java.util.StringTokenizer;

public class FastASeqStringParser extends SeqStringParser {

	public FastASeqStringParser(short prefixLength, char minQual) {
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
		// Everything but the '>'
		seq.setName(str.substring(1));

		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");

		// Read the sequence bases
		while(st.hasMoreTokens()) 
			str += st.nextToken();

		seq.setLength(str.length());
		seq.setPrefixLength(this.get_prefixLength());
		seq.setKey(str, this.get_minQual());
		seq.setSuffix(str, this.get_minQual());

		return seq.getLength();
	}
}