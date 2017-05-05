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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
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
	private static final int T_CLUSTER = 1;
	private static final NullWritable nullKey = NullWritable.get();
	private static final List<Sequence> sequences = new ArrayList<Sequence>();
	private static long maxInputClusterSize;
	private static long maxOutputClusterSize;
	private static double maxInputClusterTime;
	private static long maxInputClusterTimeSize;
	private static long maxOutputClusterTimeSize;
	private static double maxClusterTime;
	private static long numInputSequences;
	private static long numOutputSequences;
	private static long numClusters;

	private static final int fastABS(int value) {
		// Very fast abs value computation for integers
		int mask = value >> 31;
		return (value + mask) ^ mask;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration.setParameters(context.getConfiguration());
		numInputSequences = 0;
		numOutputSequences = 0;
		numClusters = 0;
		maxInputClusterSize = -1;
		maxOutputClusterSize = -1;
		maxInputClusterTime = -1;
		maxInputClusterTime = -1;
		maxInputClusterTimeSize = -1;
		maxOutputClusterTimeSize = -1;
		maxClusterTime = -1;
		
		timer.start(T_TOTAL);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		timer.stop(T_TOTAL);

		if (numInputSequences > 0) {
			System.out.format(java.util.Locale.ENGLISH,"%d/%d reads in %d clusters: %.3f sec (largest cluster %d/%d: %.3f sec, highest time %d/%d: %.3f sec)\n", numOutputSequences, numInputSequences, numClusters, timer.readTotal(T_TOTAL), maxOutputClusterSize, maxInputClusterSize, maxInputClusterTime, maxOutputClusterTimeSize, maxInputClusterTimeSize, maxClusterTime);
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		Sequence first;
		boolean insert;
		double time;
		long inputClusterSize = 1, outputClusterSize = 1;
		int diff;

		timer.start(T_CLUSTER);

		// Get the first sequence
		first = new Sequence(values.iterator().next());

		// Iterate over the remaining sequences
		for (Sequence newSeq : values) {
			inputClusterSize++;

			if (newSeq.setDist(first) > Configuration.misMatch) {
				insert = true;

				for (Sequence seq : sequences) {
					diff = fastABS(newSeq.getDist() - seq.getDist());

					if (diff <= Configuration.misMatch) {
						if(newSeq.equal(seq, Configuration.misMatch)) {
							insert = false;
							break;
						}
					}
				}
				if (insert)
					sequences.add(new Sequence(newSeq));
			}
		}

		// Print non-duplicate sequences
		context.write(nullKey, first);
		for (Sequence outSeq : sequences)
			context.write(nullKey, outSeq);

		outputClusterSize += sequences.size();
		sequences.clear();

		timer.stop(T_CLUSTER);

		// Update stats
		numInputSequences += inputClusterSize;
		numOutputSequences += outputClusterSize;
		numClusters++;
		time = timer.readLast(T_CLUSTER);

		if (inputClusterSize > maxInputClusterSize) {
			maxInputClusterSize = inputClusterSize;
			maxOutputClusterSize = outputClusterSize;
			maxInputClusterTime = time;
		}

		if (time > maxClusterTime) {
			maxInputClusterTimeSize = inputClusterSize;
			maxOutputClusterTimeSize = outputClusterSize;
			maxClusterTime = time;
		}
	}
}
