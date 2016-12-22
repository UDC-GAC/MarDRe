package es.udc.gac.mardre;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;

public class PairedEndMapper extends Mapper<LongWritable, PairedSequence, LongWritable, PairedSequence> {

	private LongWritable longKey = new LongWritable();
	private short missMatch;

	private final InMapperCombiner<LongWritable, PairedSequence> combiner = new InMapperCombiner<LongWritable, PairedSequence>(
			new CombiningFunction<LongWritable, PairedSequence>() {
				@Override
				public void combine(Map<LongWritable, List<PairedSequence>> cache, List<PairedSequence> seqsInCache, PairedSequence newSeq) {

					boolean insert = true;

					for (PairedSequence seq : seqsInCache) {
						if (newSeq.getFirstSeq().equal(seq.getFirstSeq(), missMatch, true)) {
							if (newSeq.getSecondSeq().equal(seq.getSecondSeq(), missMatch, true)) {
								insert = false;
								break;
							}
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
	public void map(LongWritable key, PairedSequence value, Context context)
			throws IOException, InterruptedException {

		longKey.set(value.getKey());
		combiner.write(longKey, value, context);
		combiner.flush(context, false);
	}
}
