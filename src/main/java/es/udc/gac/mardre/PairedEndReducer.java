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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;

public class PairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_SETUP = 1;
	private static final int T_REDUCE = 2;
	private static final int T_CLEANUP = 3;
	private static final int T_CLUSTER = 4;
	private static final NullWritable nullKey = NullWritable.get();
	private static final List<PairedSequence> sequences = new ArrayList<PairedSequence>();
	private static MultipleOutputs<NullWritable,Sequence> mos;
	private static PairedSequence firstSeq;
	private static PairedSequence iterSeq;
	private static long maxInputClusterSize;
	private static long maxOutputClusterSize;
	private static double maxInputClusterTime;
	private static long maxInputClusterTimeSize;
	private static long maxOutputClusterTimeSize;
	private static double maxClusterTime;
	private static long numInputSequences;
	private static long numOutputSequences;
	private static long numClusters;

	@Override
	public void run(Reducer<LongWritable, PairedSequence, NullWritable, Sequence>.Context context) throws IOException, InterruptedException {		

		timer.start(T_TOTAL);

		Configuration.setParameters(context.getConfiguration());
		int taskID = context.getTaskAttemptID().getTaskID().getId();

		timer.start(T_SETUP);
		setup(context);
		timer.stop(T_SETUP);

		try {
			timer.start(T_REDUCE);
			while (context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
				// If a back up store is used, reset it
				Iterator<PairedSequence> iter = context.getValues().iterator();
				if(iter instanceof ReduceContext.ValueIterator) {
					((ReduceContext.ValueIterator<PairedSequence>)iter).resetBackupStore();        
				}
			}
			timer.stop(T_REDUCE);
		} finally {
			timer.start(T_CLEANUP);
			cleanup(context);
			timer.stop(T_CLEANUP);
		}

		timer.stop(T_TOTAL);

		// Report stats
		System.out.format(java.util.Locale.ENGLISH,"Total time for reducer task %d: %.3f sec\n", taskID, timer.readTotal(T_TOTAL));
		System.out.format(java.util.Locale.ENGLISH,"\tSetup time: %.3f sec\n", timer.readTotal(T_SETUP));
		System.out.format(java.util.Locale.ENGLISH,"\tReduce time: %.3f sec\n", timer.readTotal(T_REDUCE));
		System.out.format(java.util.Locale.ENGLISH,"\tCleanup time: %.3f sec\n", timer.readTotal(T_CLEANUP));

		if (numInputSequences > 0) {
			System.out.format(java.util.Locale.ENGLISH,"\n%d/%d reads in %d clusters: %.3f sec "
					+ "(largest cluster %d/%d: %.3f sec, highest time %d/%d: %.3f sec)\n",
					numOutputSequences, numInputSequences, numClusters, timer.readTotal(T_REDUCE),
					maxOutputClusterSize, maxInputClusterSize, maxInputClusterTime, maxOutputClusterTimeSize,
					maxInputClusterTimeSize, maxClusterTime);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable,Sequence>(context);
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
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	@Override
	public void reduce(LongWritable key, Iterable<PairedSequence> values, Context context)
			throws IOException, InterruptedException {

		int inputClusterSize = 0, outputClusterSize = 0;
		double time;
		boolean insert;
		int diff, j;

		timer.start(T_CLUSTER);

		// Get the first sequence
		firstSeq = new PairedSequence(values.iterator().next());

		// Iterate over the remaining sequences
		for (PairedSequence newSeq : values) {
			inputClusterSize++;

			if (newSeq.setDist(firstSeq) > Configuration.misMatch) {
				insert = true;

				for (j=0; j<outputClusterSize; j++) {
					iterSeq = sequences.get(j);
					diff = Configuration.fastABS(newSeq.getDist() - iterSeq.getDist());

					if (diff <= Configuration.misMatch) {
						if(newSeq.equal(iterSeq, Configuration.misMatch)) {
							insert = false;

							if (newSeq.getAvgQual() > iterSeq.getAvgQual())
								// Update existing element in the list
								iterSeq.set(newSeq);

							break;
						}
					}
				}

				if (insert) {
					if(outputClusterSize < sequences.size()) {
						// Replace existing element in the list
						sequences.get(outputClusterSize).set(newSeq);
					} else {
						// Not enough space in the list
						sequences.add(new PairedSequence(newSeq));
					}
					outputClusterSize++;
				}
			}
		}

		// Write non-duplicate sequences including the first one
		context.write(nullKey, firstSeq.getFirstSeq());
		mos.write("output2", nullKey, firstSeq.getSecondSeq(), Configuration.OUTPUT_PATH_2);
		for (j=0; j<outputClusterSize; j++) {
			iterSeq = sequences.get(j);
			context.write(nullKey, iterSeq.getFirstSeq());
			mos.write("output2", nullKey, iterSeq.getSecondSeq(), Configuration.OUTPUT_PATH_2);
		}

		timer.stop(T_CLUSTER);

		// Update stats taking into account the first sequence
		numInputSequences += inputClusterSize + 1;
		numOutputSequences += outputClusterSize + 1;
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
