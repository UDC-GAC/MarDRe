package es.udc.gac.pardre.java;

import java.io.IOException;
import java.util.ArrayList;

import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SeqFileParser;
import es.udc.gac.pardre.util.Sequence;

public class ParDRe_Java {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("In ParDRe_Java");

		long startTime = System.currentTimeMillis();
		long time, partialTime;

		Options options = new Options();
		if(!options.parse(args)){
			options.printUsage("ParDRe_Java");
			return;
		}

		SeqFileParser parser = new SeqFileParser(options);
		ArrayList<Sequence> sequences = new ArrayList<Sequence>();

		partialTime = System.currentTimeMillis();

		// The method getSeq returns 0 if there are no more sequences in the file
		while(true){
			Sequence seq = new Sequence();
			if(parser.getSeq(seq) == 0){
				break;
			}

			sequences.add(seq);
		}

		time = System.currentTimeMillis()-partialTime;
		System.out.println("Read " + sequences.size() + " sequences in " + ((float) time)/1000 + " seconds");
		partialTime = System.currentTimeMillis();

		for(int i=0; i<sequences.size()-1; i++){
			for(int j=i+1; j<sequences.size(); j++){
				// In this case I indicate with true that we should also look at the prefix.
				// In general comparisons within the clusters we have to indicate false.
				if(sequences.get(i).equal(sequences.get(j), options.get_missMatch(), true)){
					// Delete the second one.
					// In the future we must delete the one with the lowest priority
					sequences.remove(j);
					j--;
				}
			}
		}

		time = System.currentTimeMillis()-partialTime;
		System.out.println("Remove duplicate in " + ((float) time)/1000 + " seconds");

		for(int i=0; i<sequences.size(); i++){
			parser.printSeq(sequences.get(i));
		}

		time = System.currentTimeMillis()-partialTime;
		System.out.println("Print " + sequences.size() + " sequences in " + ((float) time)/1000 + " seconds");

		time = System.currentTimeMillis() - startTime;

		System.out.println("Done in " + ((float) time)/1000 + " seconds");
	}
}
