package es.udc.gac.pardre.util;

public class Options {

	private String _readFileName1;
	private String _readFileName2;
	private String _writeFileName1;
	private String _writeFileName2;
	private boolean _paired;
	private char _minQual;
	private short _missMatch;
	private short _prefixLength;

	public Options() {
		_readFileName1 = null;
		_readFileName2 = null;
		_writeFileName1 = null;
		_writeFileName2 = null;
		_paired = false;
		_minQual = '0';
		_missMatch = 0;
		_prefixLength = 20;
	}

	public String getReadFileName1() {
		return _readFileName1;
	}

	public void setReadFileName1(String readFileName) {
		_readFileName1 = readFileName;
	}

	public String getReadFileName2() {
		return _readFileName2;
	}

	public void setReadFileName2(String readFileName) {
		_readFileName2 = readFileName;
	}

	public String getWriteFileName1() {
		return _writeFileName1;
	}

	public void setWriteFileName1(String writeFileName) {
		_writeFileName1 = writeFileName;
	}

	public String getWriteFileName2() {
		return _writeFileName2;
	}

	public void setWriteFileName2(String writeFileName) {
		_writeFileName2 = writeFileName;
	}

	public boolean isPaired() {
		return _paired;
	}

	public char getMinQual() {
		return _minQual;
	}

	public void setMinQual(char minQual) {
		_minQual = minQual;
	}

	public short getMissMatch() {
		return _missMatch;
	}

	public void setMissMatch(short missMatch) {
		_missMatch = missMatch;
	}

	public short getPrefixLength() {
		return _prefixLength;
	}

	public void setPrefixLength(short prefixLength) {
		_prefixLength = prefixLength;
	}

	public void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + name + " -i readFile1 [-p readFile2] [options]");
		System.out.println(
				"WARNING: Current version does not use qualities for selecting the representative sequence when duplicated");
		System.out.println("Input:");
		System.out.println(
				"    -i <string> readFile (sequence file in FASTQ format)");
		System.out.println(
				"    -p <string> readFile2 (sequence file in FASTQ format for paired scenarios)");
		System.out.println("Computation:");
		System.out.print(
				"    -m <short> (number of allowed missmatches, default ");
		System.out.print(_missMatch);
		System.out.println(")");
		System.out.print(
				"    -l <short> (prefix length used for clustering, default ");
		System.out.print(_prefixLength);
		System.out.println(")");
		System.out.print(
				"    -q <char> (quality threshold under which bases are considered 'N', default ");
		System.out.print(_minQual);
		System.out.println(")");
		System.out.println("Others:");
		System.out.println("    -h (print out the usage of the program)");
		System.out.println();
	}

	public boolean parse(String[] args) {
		int argInd = 0;

		if (args.length < 2) {
			System.out.println("Not enough parameters");
			return false;
		}

		while (argInd < args.length) {
			// Single-end sequence files
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					_readFileName1 = args[argInd];
					_writeFileName1 = args[argInd];
					_writeFileName1 = _writeFileName1.concat(".NonDup");
					argInd++;
				} else {
					System.out.println("No value specified for parameter -i");
					return false;
				}
			} else if (args[argInd].equals("-p")) {
				argInd++;
				if (argInd < args.length) {
					_readFileName2 = args[argInd];
					_writeFileName2 = args[argInd];
					_writeFileName2 = _writeFileName2.concat(".NonDup");
					_paired = true;
					argInd++;
				} else {
					System.out.println("No value specified for parameter -p");
					return false;
				}
			} else if (args[argInd].equals("-m")) {
				// Number of missmatches
				argInd++;
				if (argInd < args.length) {
					_missMatch = Short.parseShort(args[argInd]);

					if (_missMatch < 0)
						_missMatch = 0;

					argInd++;
				} else {
					System.out.println("No value specified for parameter -m");
					return false;
				}
			} else if (args[argInd].equals("-l")) {
				// Prefix length
				argInd++;
				if (argInd < args.length) {
					_prefixLength = Short.parseShort(args[argInd]);
					if (_prefixLength < 1)
						_prefixLength = 1;

					if (_prefixLength > 27) {
						System.out.println("WARNING: The length of the prefix can not be higher than 27. Using 27 instead");
						_prefixLength = 27;
					}

					argInd++;
				} else {
					System.out.println("No value specified for parameter -l");
					return false;
				}
			} else if (args[argInd].equals("-q")) {
				// Minimal quality score
				argInd++;
				if (argInd < args.length) {
					_minQual = args[argInd].charAt(0);
					if (_minQual < 0)
						_minQual = 0;

					argInd++;
				} else {
					System.out.println("No value specified for parameter -q");
					return false;
				}
			} else if (args[argInd].equals("-h")) {
				// Check the help
				return false;
			} else {
				System.out.println("Parameter " + args[argInd] + " is not valid");
				return false;
			}
		}

		return true;
	}
}
