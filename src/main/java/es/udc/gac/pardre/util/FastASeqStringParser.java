package es.udc.gac.pardre.util;

import java.util.StringTokenizer;

import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

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
		String name = st.nextToken().split("\\s+")[0];

		// Read the sequence name
		// Everything but the '>'
		seq.setName(name.substring(1));

		if (!st.hasMoreTokens())
			throw new IllegalArgumentException(
					"Wrong Sequence format for string " + s
					+ ": only name available");

		// Read the sequence bases
		String bases = new String();
		while(st.hasMoreTokens())
			bases += st.nextToken();

		seq.setFormat(FileFormat.FILE_FORMAT_FASTA);
		seq.setLength(bases.length());
		seq.setPrefixLength(this.get_prefixLength());
		seq.setKey(bases, this.get_minQual());
		seq.setSuffix(bases, this.get_minQual());

		return seq.getLength();
	}
}