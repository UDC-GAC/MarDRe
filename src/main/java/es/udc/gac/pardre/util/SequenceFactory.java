package es.udc.gac.pardre.util;

public class SequenceFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private SequenceFactory() {
		
	}
	
	public static SeqStringParser createParser(FileFormat format, short prefixLength, char minQual) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSeqStringParser(prefixLength, minQual);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastQSeqStringParser(prefixLength, minQual);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static Sequence createSequence(FileFormat format) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSequence();
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastQSequence();
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static Sequence createSequence(FileFormat format, Sequence seq) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSequence((FastQSequence) seq);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastQSequence((FastQSequence) seq);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static PairedSequence createPairedSequence(FileFormat format) {

		PairedSequence pairedSeq;

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			pairedSeq = new PairedSequence();
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			pairedSeq = new PairedSequence();
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		pairedSeq.setFormat(format);
		return pairedSeq;
	}

	public static PairedSequence createPairedSequence(FileFormat format, Sequence first, Sequence second) {

		PairedSequence pairedSeq;

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			pairedSeq = new PairedSequence(first, second);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			pairedSeq = new PairedSequence(first, second);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");

		pairedSeq.setFormat(format);
		return pairedSeq;
	}
}