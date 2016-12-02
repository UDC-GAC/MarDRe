package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.SequenceFactory;
import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

public class JoinPairedEndMapper extends Mapper<LongWritable, TupleWritable, LongWritable, PairedSequence> {

	private PairedSequence pairedSeq = new PairedSequence();
	private Sequence seq1 = new Sequence();
	private Sequence seq2 = new Sequence();
	private LongWritable longKey = new LongWritable();
	private SeqStringParser parser;
	private short prefixLength;
	private char minQual;
	private short missMatch;
	private FileFormat format;

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
	public void map(LongWritable key, TupleWritable value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.get(0).toString(), seq1) != 0) {
			if (parser.getSeq(value.get(1).toString(), seq2) != 0) {
				pairedSeq.setFirstSeq(seq1);
				pairedSeq.setSecondSeq(seq2);
				longKey.set(pairedSeq.getKey());
				combiner.write(longKey, pairedSeq, context);
			}
		}

		combiner.flush(context, false);		
	}
}
