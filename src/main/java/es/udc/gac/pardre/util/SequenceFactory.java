package es.udc.gac.pardre.util;

public class SeqStringParserFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	public static SeqStringParser createSeqStringParser(FileFormat format, short prefixLength, char minQual) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSeqStringParser(prefixLength, minQual);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastQSeqStringParser(prefixLength, minQual);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}
}