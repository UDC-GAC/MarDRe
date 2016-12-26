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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceFactory;
import es.udc.gac.mardre.util.SequenceFactory.FileFormat;

public class SingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private Sequence seq = new Sequence();
	private LongWritable longKey = new LongWritable();
	private SeqStringParser parser;
	private short prefixLength;
	private char minQual;
	private short missMatch;
	private FileFormat format;

	private final InMapperCombiner<LongWritable, Sequence> combiner = new InMapperCombiner<LongWritable, Sequence>(
			new CombiningFunction<LongWritable, Sequence>() {
				@Override
				public void combine(Map<LongWritable, List<Sequence>> cache, List<Sequence> seqsInCache, Sequence newSeq) {

					boolean insert = true;

					for (Sequence seq : seqsInCache) {
						if (newSeq.equal(seq, missMatch, true)) {
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
	public void setup(Context context) {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		format = FileFormat.values()[Integer.parseInt(job.get(Configuration.FILE_FORMAT))];
		prefixLength = Short.parseShort(job.get(Configuration.PREFIXLENGTH));
		minQual = (char) Integer.parseInt(job.get(Configuration.MINQUAL));
		parser = SequenceFactory.createParser(format, prefixLength, minQual);
		missMatch = Short.parseShort(job.get(Configuration.MISSMATCH));

		if (Boolean.parseBoolean(job.get(Configuration.IN_MAPPER_COMBINER_PARAM)) == false) {
			// Disable the in-Mapper combiner
			combiner.setCombiningFunction(null);
		}
		else {
			int cacheCapacity = Integer.parseInt(job.get(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE_PARAM));
			combiner.setMaxCacheCapacity(cacheCapacity);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context, true);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (parser.getSeq(value.toString(), seq) != 0) {
			longKey.set(seq.getKey());
			combiner.write(longKey, seq, context);
			combiner.flush(context, false);
		}
	}
}
