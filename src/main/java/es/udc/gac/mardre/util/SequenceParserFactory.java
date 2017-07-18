/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MarDRe.
 * 
 * MarDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MarDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SequenceParserFactory {

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private SequenceParserFactory() {}

	public static SeqStringParser createParser(FileFormat format, short prefixLength, short numBasesCompared, boolean isSecondPairedRead) {

		if (format == FileFormat.FILE_FORMAT_FASTQ)
			return new FastQSeqStringParser(prefixLength, numBasesCompared, isSecondPairedRead);
		else if (format == FileFormat.FILE_FORMAT_FASTA)
			return new FastASeqStringParser(prefixLength, numBasesCompared, isSecondPairedRead);
		else
			throw new IllegalArgumentException("Unrecognized file format ("+format+")");
	}

	public static FileFormat autoDetectFileFormat(FileSystem fs, Path file, short prefixLength) throws IOException {

		BufferedReader reader = null;
		FileFormat format = FileFormat.FILE_FORMAT_UNKNOWN;

		try {
			reader = new BufferedReader(new InputStreamReader(fs.open(file)));
		} catch (FileNotFoundException e) {
			throw e;
		}

		// Read first line
		String str = reader.readLine();

		// Look for the line where the sequence starts
		while ((str != null) && (str.charAt(0) != '@') && (str.charAt(0) != '>')) {
			str = reader.readLine();
		}

		if (str == null) {
			throw new IOException("Unrecognized file format");
		} else if (str.charAt(0) == '@') {
			format = FileFormat.FILE_FORMAT_FASTQ;
		} else if (str.charAt(0) == '>') {
			format = FileFormat.FILE_FORMAT_FASTA;        	
		} else {
			throw new IOException("Unrecognized file format");
		}

		// Read second line
		str = reader.readLine();

		if (str.length() < prefixLength) {
			throw new IllegalArgumentException("The length of the prefix "
					+ "("+prefixLength+") can not be greater than the length of the reads ("+str.length()+")");
		}

		reader.close();
		return format;
	}
}
