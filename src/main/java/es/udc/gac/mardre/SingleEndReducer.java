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
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
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

import es.udc.gac.mardre.util.ClusterStats;
import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;

public class SingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_REDUCE = 1;
	private static final int T_CLUSTER = 2;
	private static final int T_WRITE = 3;
	private static final NullWritable nullKey = NullWritable.get();
	private static List<Sequence> sequences;
	private static Sequence firstSeq;
	private static Sequence iterSeq;

	@Override
	public void run(Reducer<LongWritable, Sequence, NullWritable, Sequence>.Context context) throws IOException, InterruptedException {

		timer.start(T_TOTAL);

		Configuration.set(context.getConfiguration());
		int taskID = context.getTaskAttemptID().getTaskID().getId();
		ClusterStats.init();
		sequences = new ArrayList<Sequence>(Configuration.CLUSTER_LIST_INITIAL_CAPACITY);		

		setup(context);

		try {
			timer.start(T_REDUCE);
			while (context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
				// If a back up store is used, reset it
				Iterator<Sequence> iter = context.getValues().iterator();
				if(iter instanceof ReduceContext.ValueIterator) {
					((ReduceContext.ValueIterator<Sequence>)iter).resetBackupStore();        
				}
			}
			timer.stop(T_REDUCE);
		} finally {
			cleanup(context);
		}

		timer.stop(T_TOTAL);

		double reduceTime = timer.readTotal(T_REDUCE);

		// Report times
		System.out.format(java.util.Locale.ENGLISH,"Total time for reducer task %d: %.3f seconds (%d reads)\n", taskID, timer.readTotal(T_TOTAL), ClusterStats.getNumInputSequences());
		System.out.format(java.util.Locale.ENGLISH,"\tReduce time: %.3f seconds\n", reduceTime);
		System.out.format(java.util.Locale.ENGLISH,"\tClustering time: %.3f seconds\n", timer.readTotal(T_CLUSTER));
		System.out.format(java.util.Locale.ENGLISH,"\tWrite time: %.3f seconds\n", timer.readTotal(T_WRITE));

		// Report stats
		ClusterStats.report(reduceTime);
	}

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		int inputClusterSize = 1, outputClusterSize = 0;
		boolean insert;
		int diff, j;

		timer.start(T_CLUSTER);

		// Get the first sequence
		firstSeq = new Sequence(values.iterator().next());

		// Iterate over the remaining sequences
		for (Sequence newSeq : values) {
			inputClusterSize++;

			if (newSeq.setDist(firstSeq) > Configuration.MISMATCH) {
				insert = true;

				for (j=0; j<outputClusterSize; j++) {
					iterSeq = sequences.get(j);
					diff = Configuration.fastABS(newSeq.getDist() - iterSeq.getDist());

					if (diff <= Configuration.MISMATCH) {
						if(newSeq.equal(iterSeq, Configuration.MISMATCH)) {
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
						sequences.add(new Sequence(newSeq));
					}
					outputClusterSize++;
				}
			}
		}

		timer.stop(T_CLUSTER);

		timer.start(T_WRITE);

		// Write non-duplicate sequences including the first one
		context.write(nullKey, firstSeq);
		for (j=0; j<outputClusterSize; j++)
			context.write(nullKey, sequences.get(j));

		outputClusterSize++;

		timer.stop(T_WRITE);

		// Update stats
		ClusterStats.update(inputClusterSize, outputClusterSize, timer.readLast(T_CLUSTER), timer.readLast(T_WRITE));
	}
}
