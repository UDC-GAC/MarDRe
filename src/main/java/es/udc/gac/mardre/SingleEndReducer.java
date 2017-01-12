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

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;

public class SingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final NullWritable nullKey = NullWritable.get();
	private static final List<Sequence> sequences = new ArrayList<Sequence>();
	private static long numAnalyzedSequences;
	private static long numPrintedSequences;

	private static final int fastABS(int value) {
		// Very fast abs value computation for integers
		int mask = value >> 31;
		return (value + mask) ^ mask;
	}

	@Override
	protected void setup(Context context) {
		Configuration.setParameters(context.getConfiguration());
		numAnalyzedSequences = 0;
		numPrintedSequences = 0;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.format(java.util.Locale.ENGLISH,"Time to analyze %d/%d reads: %.3f seconds\n", numPrintedSequences, numAnalyzedSequences, timer.read(T_TOTAL));
	}

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		Sequence first, seq1, seq2;
		int diff, i, j;

		timer.start(T_TOTAL);

		// Get and write the first sequence
		first = new Sequence(values.iterator().next());
		context.write(nullKey, first);

		// Iterate over the remaining sequences
		for (Sequence newSeq : values) {
			numAnalyzedSequences++;

			if (newSeq.setDist(first) > Configuration.missMatch)
				sequences.add(new Sequence(newSeq));
		}

		for (i=0; i<sequences.size(); i++) {
			seq1 = sequences.get(i);

			for (j=i+1; j<sequences.size(); j++) {
				seq2 = sequences.get(j);
				diff = seq1.getDist() - seq2.getDist();

				if (fastABS(diff) <= Configuration.missMatch) {
					if(seq1.equal(seq1, Configuration.missMatch)) {
						sequences.remove(j);
						j--;
					}
				}
			}
			context.write(nullKey, seq1);
		}

		numPrintedSequences+=sequences.size();
		sequences.clear();

		timer.stop(T_TOTAL);
	}
}
