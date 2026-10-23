/*
 * Copyright (C) 2017-2026 Universidade da Coruña
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
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceRecordReader;
import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory;
import es.udc.gac.mardre.util.Timer;

public class PairedEndMapJoinMapper extends Mapper<LongWritable, PairText, LongWritable, PairedSequence> {

	private static final Logger LOG = LoggerFactory.getLogger(PairedEndMapJoinMapper.class);

	private final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_SETUP = 1;
	private static final int T_MAP = 2;
	private static final int T_CLEANUP = 3;
	private PairedSequence pairedSeq = new PairedSequence();
	private LongWritable longKey = new LongWritable();
	private Sequence firstSeq = new Sequence();
	private Sequence secondSeq = new Sequence();
	private SeqStringParser firstParser;
	private SeqStringParser secondParser;
	private long numInputSequences;

	private final InMapperCombiner<LongWritable, PairedSequence> combiner = new InMapperCombiner<LongWritable, PairedSequence>(
			new CombiningFunction<LongWritable, PairedSequence>() {
				@Override
				public void combine(Map<LongWritable, List<PairedSequence>> cache, List<PairedSequence> seqsInCache, PairedSequence newSeq) {

					boolean insert = true;

					for (PairedSequence seq : seqsInCache) {
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
		if (LOG.isInfoEnabled())
			LOG.info(String.format(java.util.Locale.ENGLISH, "Mapper task %d: %d reads in %.3f s (setup %.3f s, map %.3f s, cleanup %.3f s)", taskID, numInputSequences, timer.readTotal(T_TOTAL), timer.readTotal(T_SETUP), timer.readTotal(T_MAP), timer.readTotal(T_CLEANUP)));
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		numInputSequences = 0;

		firstParser = SequenceParserFactory.createParser(Configuration.FILE_FORMAT, Configuration.PREFIX_LENGTH, Configuration.NUMBER_BASES_COMPARED);
		secondParser = SequenceParserFactory.createParser(Configuration.FILE_FORMAT, (short) 0, Configuration.NUMBER_BASES_COMPARED);

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
	public void map(LongWritable key, PairText value, Context context)
			throws IOException, InterruptedException {

		firstParser.getSeq(PairedEndSequenceRecordReader.getLeftRead(value), firstSeq, true);
		secondParser.getSeq(PairedEndSequenceRecordReader.getRightRead(value), secondSeq, false);
		pairedSeq.setFirstSeq(firstSeq);
		pairedSeq.setSecondSeq(secondSeq);
		longKey.set(pairedSeq.getKey());
		combiner.write(longKey, pairedSeq, context);
		numInputSequences++;
		combiner.flush(context, false);
	}
}
