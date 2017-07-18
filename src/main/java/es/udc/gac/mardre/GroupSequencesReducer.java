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
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;

public class GroupSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_REDUCE = 1;
	private static PairedSequence pairedSeq = new PairedSequence();
	private static LongWritable longKey = new LongWritable();
	private static long numInputSequences = 0;

	@Override
	public void run(Reducer<LongWritable, Sequence, LongWritable, PairedSequence>.Context context) throws IOException, InterruptedException {

		timer.start(T_TOTAL);

		Configuration.set(context.getConfiguration());
		int taskID = context.getTaskAttemptID().getTaskID().getId();

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

		// Report stats
		System.out.format(java.util.Locale.ENGLISH,"Total time for reducer task %d: %.3f seconds (%d reads)\n", taskID, timer.readTotal(T_TOTAL), numInputSequences);
		System.out.format(java.util.Locale.ENGLISH,"\tReduce time: %.3f seconds\n", timer.readTotal(T_REDUCE));
	}

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		int i = 0;

		for (Sequence seq : values) {
			Sequence newSeq = new Sequence(seq);
			i++;

			if (seq.isFirstSeq()) {
				pairedSeq.setFirstSeq(newSeq);
			} else {
				pairedSeq.setSecondSeq(newSeq);
			}
		}

		if (i != 2)
			throw new IndexOutOfBoundsException("PairedSequence consists of "+i+" sequence/s");

		longKey.set(pairedSeq.getKey());
		context.write(longKey, pairedSeq);
		numInputSequences++;
	}
}
