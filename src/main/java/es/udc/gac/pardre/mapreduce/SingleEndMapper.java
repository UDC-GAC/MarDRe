package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.SequenceFactory;
import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

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
				public void combine(Map<LongWritable, List<Sequence>> cache, List<Sequence> sequencesInCache, Sequence newSequence) {

					boolean insert = true;
					
					for (Sequence seq : sequencesInCache) {
						if (newSequence.equal(seq, missMatch, true)) {
							insert = false;
							break;
						}
					}
					
					if (insert) {
						// Cache the sequence
						sequencesInCache.add(newSequence);
						longKey.set(newSequence.getKey());
						cache.put(longKey, sequencesInCache);
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
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.toString(), seq) != 0) {
			longKey.set(seq.getKey());
			//context.write(longKey, seq);
			combiner.write(longKey, seq, context);
		}
	}
}
