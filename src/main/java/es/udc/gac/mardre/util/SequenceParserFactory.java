/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MaRDRe.
 * 
 * MaRDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MaRDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MaRDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

public class SequenceParserFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private SequenceParserFactory() {}

	public static SeqStringParser createParser(FileFormat format, short prefixLength, char minQual) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSeqStringParser(prefixLength, minQual);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastASeqStringParser(prefixLength, minQual);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}
}
