package es.udc.gac.pardre.util;

import java.util.StringTokenizer;

import es.udc.gac.pardre.mapreduce.ParDReMR.FileFormat;

public class SeqStringParser {

	private short _prefixLength;
	private char _minQual;

	public SeqStringParser(short prefixLength, char minQual) {
		this._prefixLength = prefixLength;
		this._minQual = minQual;
	}

	public int getSeq(FileFormat fileFormat, String s, Sequence seq) {

		if (fileFormat == FileFormat.FILE_FORMAT_FASTQ)
			return getFastQSeq(s, seq);
		else if (fileFormat == FileFormat.FILE_FORMAT_FASTA)
			return getFastASeq(s, seq);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+fileFormat+")");
	}

	private int getFastQSeq(String s, Sequence seq) {

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
		// Everything but the @
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
		seq.setPrefixLength(_prefixLength);
		seq.setQuals(strQuals);
		seq.setKey(str, _minQual);
		seq.setSuffix(str, _minQual);

		return seq.getLength();
	}

	private int getFastASeq(String s, Sequence seq) {
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
		// Everything but the @
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
		seq.setPrefixLength(_prefixLength);
		seq.setQuals(strQuals);
		seq.setKey(str, _minQual);
		seq.setSuffix(str, _minQual);

		return seq.getLength();
	}
}
