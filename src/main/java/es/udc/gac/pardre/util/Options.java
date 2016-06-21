package es.udc.gac.pardre.util;

public class Options {
	private String _readFileName;
	private String _writeFileName;
	private char _minQual;
	private short _missMatch;
	private short _prefixLength;

	public Options() {
		_readFileName = null;
		_writeFileName = null;
		_minQual = 3;
		_missMatch = 0;
		_prefixLength = 20;
	}

	public String get_readFileName() {
		return _readFileName;
	}

	public void set_readFileName(String readFileName) {
		_readFileName = readFileName;
	}

	public String get_writeFileName() {
		return _writeFileName;
	}

	public void set_writeFileName(String writeFileName) {
		_writeFileName = writeFileName;
	}

	public char get_minQual() {
		return _minQual;
	}

	public void set_minQual(char minQual) {
		_minQual = minQual;
	}

	public short get_missMatch() {
		return _missMatch;
	}

	public void set_missMatch(short missMatch) {
		_missMatch = missMatch;
	}

	public short get_prefixLength() {
		return _prefixLength;
	}

	public void set_prefixLength(short prefixLength) {
		_prefixLength = prefixLength;
	}

	public void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + name + " -i readFile [options]");
		System.out.println(
				"WARNING: Current version only allows single-end datasets");
		System.out.println(
				"WARNING: In the current version we do not use qualities for selecting the representative sequence when duplicated");
		System.out.println(
				"WARNING: The current version does not support threads");
		System.out.println("Input:");
		System.out.println(
				"    -i <string> readFile (sequence file in FASTQ format)");
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

		if (args.length < 2)
		{
			System.out.println("Not enough parameters");
			return false;
		}

		// Print out the command line
		System.out.print("Command: ");
		for (int i = 0; i < args.length; i++)
		{
			System.out.print(args[i]+" ");
		}
		System.out.println();

		while (argInd < args.length) {
			// Input file
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					_readFileName = args[argInd];
					_writeFileName = args[argInd];
					_writeFileName = _writeFileName.concat(".NonDup");
					argInd++;
				} else {
					System.out.println("Not specified value for parameter -i");
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
					System.out.println("Not specified value for parameter -m");
					return false;
				}
			} else if (args[argInd].equals("-l")) {
				// Prefix length
				argInd++;
				if (argInd < args.length) {
					_prefixLength = Short.parseShort(args[argInd]);
					if (_prefixLength < 1)
						_prefixLength = 1;

					if (_prefixLength > 27)
						_prefixLength = 27;

					argInd++;
				} else {
					System.out.println("Not specified value for parameter -l");
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
					System.out.println("Not specified value for parameter -q");
					return false;
				}
			} else if (args[argInd].equals("-h")) {
				// Check the help
				return false;
			} else {
				System.out.println("Parameter " + args[argInd] + " not valid");
				return false;
			}
		}

		return true;
	}
}