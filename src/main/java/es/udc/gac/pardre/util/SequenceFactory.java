package es.udc.gac.pardre.util;

public class SequenceFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private SequenceFactory() {}

	public static SeqStringParser createParser(FileFormat format, short prefixLength, char minQual) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSeqStringParser(prefixLength, minQual);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastQSeqStringParser(prefixLength, minQual);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static Sequence createSequence(FileFormat format) {

		if (format != FileFormat.FILE_FORMAT_FASTQ && format != FileFormat.FILE_FORMAT_FASTA)
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		return new Sequence();
	}

	public static Sequence createSequence(FileFormat format, Sequence seq) {

		if (format != FileFormat.FILE_FORMAT_FASTQ && format != FileFormat.FILE_FORMAT_FASTA)
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		return new Sequence(seq);
	}

	public static PairedSequence createPairedSequence(FileFormat format) {

		if (format != FileFormat.FILE_FORMAT_FASTQ && format != FileFormat.FILE_FORMAT_FASTA)
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		PairedSequence pairedSeq = new PairedSequence();
		pairedSeq.setFormat(format);
		return pairedSeq;
	}

	public static PairedSequence createPairedSequence(FileFormat format, Sequence first, Sequence second) {

		if (format != FileFormat.FILE_FORMAT_FASTQ && format != FileFormat.FILE_FORMAT_FASTA)
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		PairedSequence pairedSeq = new PairedSequence(first, second);
		pairedSeq.setFormat(format);
		return pairedSeq;
	}
}
