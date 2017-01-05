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
package es.udc.gac.mardre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;

public class PairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

	private static final NullWritable nullKey = NullWritable.get();
	private static final String path2 = Configuration.TEMP_OUTPUT_PATH_2+"/part";
	private MultipleOutputs<NullWritable,Sequence> mos;
	private short missMatch;	

	@Override
	public void setup(Context context) {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		missMatch = Short.parseShort(job.get(Configuration.MISSMATCH));
		mos = new MultipleOutputs<NullWritable,Sequence>(context);
	}

	@Override
	public void cleanup(Context context ) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	public void reduce(LongWritable key, Iterable<PairedSequence> values, Context context)
			throws IOException, InterruptedException {

		List<PairedSequence> sequences = new ArrayList<PairedSequence>();

		// Get and write the first sequence
		PairedSequence first = new PairedSequence(values.iterator().next());
		context.write(nullKey, first.getFirstSeq());
		mos.write("output2", nullKey, first.getSecondSeq(), Configuration.TEMP_OUTPUT_PATH_2+"/part");

		// Iterate over the remaining sequences
		for (PairedSequence newSeq : values) {
			if (newSeq.setDist(first) > missMatch)
				sequences.add(new PairedSequence(newSeq));
		}

		PairedSequence seq1, seq2;
		int diff, j, i, mask;

		// Remove duplicates
		for (i=0; i<sequences.size()-1; i++) {
			seq1 = sequences.get(i);

			for (j=i+1; j<sequences.size(); j++) {
				seq2 = sequences.get(j);
				diff = seq1.getDist() - seq2.getDist();
				// Very fast abs computation for integers
				mask = diff >> 31;
                                diff = (diff + mask) ^ mask;

				if (diff <= missMatch) {
					if(seq1.equal(seq2, missMatch)) {
						sequences.remove(j);
						j--;
					}
				}
			}

			context.write(nullKey, seq1.getFirstSeq());
			mos.write("output2", nullKey, seq1.getSecondSeq(), path2);
		}

		sequences = null;
		first = null;
	}
}
