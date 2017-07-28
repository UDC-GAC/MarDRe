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

import es.udc.gac.mardre.RunMarDRe;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class Options {

	private String _readFileName1;
	private String _readFileName2;
	private String _writeFileName1;
	private String _writeFileName2;
	private FileFormat _inputFormat;
	private boolean _paired;
	private short _misMatch;
	private short _prefixLength;
	private short _numBasesCompared;
	private int _nreducers;

	public Options() {
		_readFileName1 = null;
		_readFileName2 = null;
		_writeFileName1 = null;
		_writeFileName2 = null;
		_inputFormat = FileFormat.FILE_FORMAT_UNKNOWN;
		_paired = false;
		_misMatch = 0;
		_prefixLength = 20;
		_numBasesCompared = -1;
		_nreducers = 1;
	}

	public String getReadFileName1() {
		return _readFileName1;
	}

	public String getReadFileName2() {
		return _readFileName2;
	}

	public String getWriteFileName1() {
		return _writeFileName1;
	}

	public String getWriteFileName2() {
		return _writeFileName2;
	}

	public FileFormat getInputFileFormat() {
		return _inputFormat;
	}

	public boolean isPaired() {
		return _paired;
	}

	public short getMisMatch() {
		return _misMatch;
	}

	public short getPrefixLength() {
		return _prefixLength;
	}

	public short getNumberOfBasesCompared() {
		return _numBasesCompared;
	}

	public int getNumberOfReducers() {
		return _nreducers;
	}

	public static void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + name + " -i inputFile1 [-o outputFile1] [-p inputFile1] [-r outputFile2] [options]");
		System.out.println("Input:");
		System.out.println(
				"    -i <string> inputFile1 (input file in FASTQ/FASTA format)");
		System.out.println(
				"    -p <string> inputFile2 (input file in FASTQ/FASTA format for paired-end scenarios)");
		System.out.println(
				"    -q (Input sequence files are in FASTQ format, default: autodetect)");
		System.out.println(
				"    -f (Input sequence files are in FASTA format, default: autodetect)");
		System.out.println("Output:");
		System.out.println(
				"    -o <string> outputFile1 (output file in FASTQ/FASTA format, default = inputFile1.NonDup)");
		System.out.println(
				"    -r <string> outputFile2 (output file in FASTQ/FASTA format for paired-end scenarios, default = inputFile2.NonDup)");
		System.out.println("Computation:");
		System.out.println(
				"    -m <integer> (number of allowed mismatches, default 0)");
		System.out.println(
				"    -l <integer> (prefix length used for clustering, default 20)");
		System.out.println(
				"    -c <integer> (number of bases to compare for each read starting from the beginning, default is equal to the sequence length)");
		System.out.println(
				"    -nr <integer> (number of reducers, default 1)");
		System.out.println("Others:");
		System.out.println(
				"    -h (print out the usage of the program)");
		System.out.println(
				"    -v (print out the version of the program)");
	}

	public boolean parse(String[] args) {
		int argInd = 0;
		boolean withInputFile = false;
		boolean withOutputFile = false;
		boolean withPairedOutputFile = false;

		while (argInd < args.length) {
			// Single-end sequence files
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					_readFileName1 = args[argInd];
					withInputFile = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -i");
					return false;
				}
			} else if (args[argInd].equals("-p")) {
				argInd++;
				if (argInd < args.length) {
					_readFileName2 = args[argInd];
					_paired = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -p");
					return false;
				}
			} else if (args[argInd].equals("-o")) {
				argInd++;
				if (argInd < args.length) {
					_writeFileName1 = args[argInd];
					withOutputFile = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -o");
					return false;
				}
			} else if (args[argInd].equals("-r")) {
				argInd++;
				if (argInd < args.length) {
					_writeFileName2 = args[argInd];
					withPairedOutputFile = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -r");
					return false;
				}
			} else if (args[argInd].equals("-q")) {
				argInd++;
				_inputFormat = FileFormat.FILE_FORMAT_FASTQ;
			} else if (args[argInd].equals("-f")) {
				argInd++;
				_inputFormat = FileFormat.FILE_FORMAT_FASTA;
			} else if (args[argInd].equals("-nr")) {
				// Number of reducers
				argInd++;
				if (argInd < args.length) {
					_nreducers = Short.parseShort(args[argInd]);

					if (_nreducers < 1) {
						System.out.println("MarDRe-WARNING: The number of reducers "
								+ "(-m "+_nreducers+") can not be less than 1. Using -nr 1 instead");
						_nreducers = 1;
					}

					argInd++;
				} else {
					System.err.println("No value specified for parameter -nr");
					return false;
				}
			} else if (args[argInd].equals("-m")) {
				// Number of mismatches
				argInd++;
				if (argInd < args.length) {
					_misMatch = Short.parseShort(args[argInd]);

					if (_misMatch < 0) {
						System.out.println("MarDRe-WARNING: The number of allowed mismatches "
								+ "(-m "+_misMatch+") can not be less than 0. Using -m 0 instead");
						_misMatch = 0;
					}

					argInd++;
				} else {
					System.err.println("No value specified for parameter -m");
					return false;
				}
			} else if (args[argInd].equals("-l")) {
				// Prefix length
				argInd++;
				if (argInd < args.length) {
					_prefixLength = Short.parseShort(args[argInd]);

					if (_prefixLength < 1) {
						System.out.println("MarDRe-WARNING: The length of the prefix "
								+ "(-l "+_prefixLength+") can not be less than 1. Using -l 20 instead");
						_prefixLength = 20;
					} else if (_prefixLength > 27) {
						System.out.println("MarDRe-WARNING: The length of the prefix "
								+ "(-l "+_prefixLength+") can not be greater than 27. Using -l 27 instead");
						_prefixLength = 27;
					}

					argInd++;
				} else {
					System.err.println("No value specified for parameter -l");
					return false;
				}
			} else if (args[argInd].equals("-c")) {
				// Number of bases to compare
				argInd++;
				if (argInd < args.length) {
					_numBasesCompared = Short.parseShort(args[argInd]);

					if (_numBasesCompared < 1) {
						System.out.println("MarDRe-WARNING: The number of bases to compare "
								+ "(-c "+_numBasesCompared+") can not be less than 1. Using -c 1 instead");
						_numBasesCompared = 1;
					}

					argInd++;
				} else {
					System.err.println("No value specified for parameter -c");
					return false;
				} 
			} else if (args[argInd].equals("-h")) {
				// Check the help
				Options.printUsage(RunMarDRe.class.getName());
				return false;
			} else if (args[argInd].equals("-v")) {
				// Check the version
				System.out.println("MarDRe: version "+Configuration.MARDRE_VERSION);
				System.out.println("MarDRe: more info available at "+Configuration.MARDRE_WEBPAGE);
				return false;
			} else {
				System.err.println("Parameter " + args[argInd] + " is not valid");
				return false;
			}
		}

		if (!withInputFile) {
			System.err.println("No input file specified with -i");
			return false;
		}

		if((_numBasesCompared > 0) && (_numBasesCompared < _prefixLength)){
			System.out.println("MarDRe-WARNING: The number of bases to compare (-c "+_numBasesCompared+") must be at least as high as the prefix length (-l "+_prefixLength+"). Using -c "+_prefixLength+" instead");
			_numBasesCompared = _prefixLength;
		}

		if (!withOutputFile)
			_writeFileName1 = _readFileName1+".NonDup";

		if (!withPairedOutputFile && _paired)
			_writeFileName2 = _readFileName2+".NonDup";

		return true;
	}
}
