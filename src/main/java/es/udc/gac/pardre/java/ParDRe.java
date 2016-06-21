package es.udc.gac.pardre.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SeqFileParser;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.Timer;

public class ParDRe {

	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_READ_SEQ = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_PRINT_SEQ = 4;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Timer timer = new Timer();

		System.out.println("Starting ParDRe");

		timer.start(T_TOTAL);

		timer.start(T_INIT);
		Options options = new Options();
		if(!options.parse(args)) {
			options.printUsage("ParDRe");
			return;
		}

		SeqFileParser parser = new SeqFileParser(options);
		List<Sequence> sequences = new ArrayList<Sequence>();
		timer.stop(T_INIT);

		timer.start(T_READ_SEQ);
		// The method getSeq returns 0 if there are no more sequences in the file
		while(true) {
			Sequence seq = new Sequence();
			if(parser.getSeq(seq) == 0){
				break;
			}
			sequences.add(seq);
		}
		timer.stop(T_READ_SEQ);

		timer.start(T_REMOVE_DUP);
		for(int i=0; i<sequences.size()-1; i++) {
			for(int j=i+1; j<sequences.size(); j++) {
				// In this case I indicate with true that we should also look at the prefix.
				// In general comparisons within the clusters we have to indicate false.
				if(sequences.get(i).equal(sequences.get(j), options.get_missMatch(), true)) {
					// Delete the second one.
					// In the future we must delete the one with the lowest priority
					sequences.remove(j);
					j--;
				}
			}
		}
		timer.stop(T_REMOVE_DUP);

		timer.start(T_PRINT_SEQ);
		for(int i=0; i<sequences.size(); i++) {
			parser.printSeq(sequences.get(i));
		}
		timer.stop(T_PRINT_SEQ);

		timer.stop(T_TOTAL);

		System.out.format(java.util.Locale.ENGLISH,"Initialized in %.3f seconds\n", timer.read(T_INIT));
		System.out.format(java.util.Locale.ENGLISH,"Read %d sequences in %.3f seconds\n", sequences.size(), timer.read(T_READ_SEQ));
		System.out.format(java.util.Locale.ENGLISH,"Remove duplicate in %.3f seconds\n", timer.read(T_REMOVE_DUP));
		System.out.format(java.util.Locale.ENGLISH,"Print %d sequences in %.3f seconds\n", sequences.size(), timer.read(T_PRINT_SEQ));
		System.out.format(java.util.Locale.ENGLISH,"Done in %.3f seconds\n", timer.read(T_TOTAL));
		System.exit(0);
	}
}