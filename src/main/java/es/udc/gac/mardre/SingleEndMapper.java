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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.gac.mardre.util.Timer;
import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory;

public class SingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_SETUP = 1;
	private static final int T_MAP = 2;
	private static final int T_CLEANUP = 3;
	private static Sequence seq = new Sequence();
	private static LongWritable longKey = new LongWritable();
	private static SeqStringParser parser;
	private static long numInputSequences;

	private final InMapperCombiner<LongWritable, Sequence> combiner = new InMapperCombiner<LongWritable, Sequence>(
			new CombiningFunction<LongWritable, Sequence>() {
				@Override
				public void combine(Map<LongWritable, List<Sequence>> cache, List<Sequence> seqsInCache, Sequence newSeq) {

					boolean insert = true;

					for (Sequence seq : seqsInCache) {
						if (newSeq.equal(seq, Configuration.MISMATCH)) {
							insert = false;
							break;
						}
					}

					if (insert) {
						// Put the sequence into the cache
						seqsInCache.add(newSeq);
						longKey.set(newSeq.getKey());
						cache.put(longKey, seqsInCache);
					}
				}
			}
			);

	@Override
	public void run(Context context) throws IOException, InterruptedException {

		timer.start(T_TOTAL);

		Configuration.set(context.getConfiguration());
		int taskID = context.getTaskAttemptID().getTaskID().getId();

		timer.start(T_SETUP);
		setup(context);
		timer.stop(T_SETUP);

		try {
			timer.start(T_MAP);
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			timer.stop(T_MAP);
		} finally {
			timer.start(T_CLEANUP);
			cleanup(context);
			timer.stop(T_CLEANUP);
		}

		timer.stop(T_TOTAL);

		// Report stats
		System.out.format(java.util.Locale.ENGLISH,"Total time for mapper task %d: %.3f seconds (%d reads)\n", taskID, timer.readTotal(T_TOTAL), numInputSequences);
		System.out.format(java.util.Locale.ENGLISH,"\tSetup time: %.3f seconds\n", timer.readTotal(T_SETUP));
		System.out.format(java.util.Locale.ENGLISH,"\tMap time: %.3f seconds\n", timer.readTotal(T_MAP));
		System.out.format(java.util.Locale.ENGLISH,"\tCleanup time: %.3f seconds\n", timer.readTotal(T_CLEANUP));
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		parser = SequenceParserFactory.createParser(Configuration.FILE_FORMAT, Configuration.PREFIX_LENGTH, Configuration.NUMBER_BASES_COMPARED, false);
		numInputSequences = 0;

		if (Configuration.IN_MAPPER_COMBINER == false) {
			// Disable the in-Mapper combiner
			combiner.setCombiningFunction(null);
		} else {
			combiner.setMaxCacheCapacity(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context, true);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String str = Text.decode(value.getBytes(), 0, value.getLength(), false);
		parser.getSeq(str, seq);
		longKey.set(seq.getKey());
		combiner.write(longKey, seq, context);
		numInputSequences++;
		combiner.flush(context, false);
	}
}
