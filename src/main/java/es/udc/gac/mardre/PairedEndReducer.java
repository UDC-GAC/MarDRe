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
import es.udc.gac.mardre.util.Timer;

public class PairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final NullWritable nullKey = NullWritable.get();
	private static final List<PairedSequence> sequences = new ArrayList<PairedSequence>();
	private static MultipleOutputs<NullWritable,Sequence> mos;
	private static long maxInputClusterSize;
	private static long maxOutputClusterSize;
	private static long numInputSequences;
	private static long numOutputSequences;
	private static double maxTime;

	private static final int fastABS(int value) {
		// Very fast abs value computation for integers
		int mask = value >> 31;
		return (value + mask) ^ mask;
	}

	@Override
	protected void setup(Context context) {
		Configuration.setParameters(context.getConfiguration());
		mos = new MultipleOutputs<NullWritable,Sequence>(context);
		numInputSequences = 0;
		numOutputSequences = 0;
		maxInputClusterSize = -1;
		maxOutputClusterSize = -1;
		maxTime = -1;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.format(java.util.Locale.ENGLISH,"Time to analyze %d/%d reads: %.3f sec (largest cluster %d/%d in %.3f sec)\n", numOutputSequences, numInputSequences, timer.readTotal(T_TOTAL), maxOutputClusterSize, maxInputClusterSize, maxTime);
		mos.close();
	}

	@Override
	public void reduce(LongWritable key, Iterable<PairedSequence> values, Context context)
			throws IOException, InterruptedException {

		long inputClusterSize = 0, outputClusterSize = 0;
		int diff;
		boolean insert;

		timer.start(T_TOTAL);

		// Get and write the first sequence
		PairedSequence first = new PairedSequence(values.iterator().next());
		context.write(nullKey, first.getFirstSeq());
		mos.write("output2", nullKey, first.getSecondSeq(), Configuration.OUTPUT_PATH_2);

		// Iterate over the remaining sequences
		for (PairedSequence newSeq : values) {
			inputClusterSize++;
			numInputSequences++;

			if (newSeq.setDist(first) > Configuration.missMatch) {
				insert = true;

				for (PairedSequence seq : sequences) {
					diff = newSeq.getDist() - seq.getDist();

					if (fastABS(diff) <= Configuration.missMatch) {
						if(newSeq.equal(seq, Configuration.missMatch)) {
							insert = false;
							break;
						}
					}
				}
				if (insert)
					sequences.add(new PairedSequence(newSeq));
			}
		}

		// Print non-duplicated sequences
		for (PairedSequence seq : sequences) {
			context.write(nullKey, seq.getFirstSeq());
			mos.write("output2", nullKey, seq.getSecondSeq(), Configuration.OUTPUT_PATH_2);
		}

		outputClusterSize = sequences.size();
		sequences.clear();

		timer.stop(T_TOTAL);

		// Update stats
		numOutputSequences+=outputClusterSize;

		if (inputClusterSize > maxInputClusterSize) {
			maxInputClusterSize = inputClusterSize;
			maxOutputClusterSize = outputClusterSize;
			maxTime = timer.readLast(T_TOTAL);
		}
	}
}
