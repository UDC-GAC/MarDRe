package es.udc.gac.pardre.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SeqFileParser {

	private BufferedReader _fileBufferR;
	private BufferedWriter _fileBufferW;

	private short _prefixLength;
	private char _minQual;

	public SeqFileParser(Options options) {
		try {
			_fileBufferR = new BufferedReader(
					new FileReader(options.get_readFileName()));
			_fileBufferW = new BufferedWriter(
					new FileWriter(options.get_writeFileName()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		_prefixLength = options.get_prefixLength();
		_minQual = options.get_minQual();
	}

	// Returns the size of the sequence or 0 if there are no more sequences in
	// the file
	public int getSeq(Sequence seq) throws IOException {
		String str = _fileBufferR.readLine();
		String strQuals;

		// Look for the line where the sequence starts
		while ((str != null) && (str.charAt(0) != '@')) {
			str = _fileBufferR.readLine();
		}

		if (str == null)
			return 0;

		// Read the sequence name
		// Everything but the @
		seq.set_name(str.substring(1));

		// Read the sequence bases
		str = _fileBufferR.readLine();
		if (str == null) {
			throw new IOException(
					"Sequence incomplete in FASTQ file: Only name available");
		}

		// Read the comment line
		strQuals = _fileBufferR.readLine();
		if (strQuals == null) {
			throw new IOException(
					"Sequence incomplete in FASTQ file: No comment available ");
		}

		// Read the qualities
		strQuals = _fileBufferR.readLine();
		if (strQuals == null) {
			throw new IOException(
					"Sequence incomplete in FASTQ file: No qualities available ");
		}

		seq.set_length(str.length());
		seq.set_prefix_length(_prefixLength);
		seq.set_quals(strQuals);
		seq.setKey(str, _minQual);
		seq.setSuffix(str, _minQual);

		return seq.get_length();
	}

	public void printSeq(Sequence seq) {
		try {
			seq.print(_fileBufferW);
			_fileBufferW.newLine();
			_fileBufferW.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}