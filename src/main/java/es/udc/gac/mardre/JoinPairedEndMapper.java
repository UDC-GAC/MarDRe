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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory;

public class JoinPairedEndMapper extends Mapper<LongWritable, TupleWritable, LongWritable, PairedSequence> {

	private static PairedSequence pairedSeq = new PairedSequence();
	private static Sequence seq1 = new Sequence();
	private static Sequence seq2 = new Sequence();
	private static LongWritable longKey = new LongWritable();
	private static SeqStringParser parser1, parser2;

	private static final InMapperCombiner<LongWritable, PairedSequence> combiner = new InMapperCombiner<LongWritable, PairedSequence>(
			new CombiningFunction<LongWritable, PairedSequence>() {
				@Override
				public void combine(Map<LongWritable, List<PairedSequence>> cache, List<PairedSequence> seqsInCache, PairedSequence newSeq) {

					boolean insert = true;

					for (PairedSequence seq : seqsInCache) {
						if (newSeq.equal(seq, Configuration.misMatch)) {
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
	protected void setup(Context context) throws IOException, InterruptedException {
		org.apache.hadoop.conf.Configuration jobConf = context.getConfiguration();
		Configuration.setParameters(jobConf);
		parser1 = SequenceParserFactory.createParser(Configuration.fileFormat, Configuration.prefixLength);
		parser2 = SequenceParserFactory.createParser(Configuration.fileFormat, (short) 0);

		if (Boolean.parseBoolean(jobConf.get(Configuration.IN_MAPPER_COMBINER_PARAM)) == false) {
			// Disable the in-Mapper combiner
			combiner.setCombiningFunction(null);
		} else {
			int cacheCapacity = Integer.parseInt(jobConf.get(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE_PARAM));
			combiner.setMaxCacheCapacity(cacheCapacity);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context, true);
	}

	@Override
	public void map(LongWritable key, TupleWritable value, Context context)
			throws IOException, InterruptedException {

		if (parser1.getSeq(value.get(0).toString(), seq1) != 0) {
			if (parser2.getSeq(value.get(1).toString(), seq2) != 0) {
				pairedSeq.setFirstSeq(seq1);
				pairedSeq.setSecondSeq(seq2);
				longKey.set(pairedSeq.getKey());
				combiner.write(longKey, pairedSeq, context);
				combiner.flush(context, false);
			}
		}
	}
}
