package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import es.udc.gac.pardre.util.PairedSequence;

public class PairedEndMapper extends Mapper<LongWritable, PairedSequence, LongWritable, PairedSequence> {

	private LongWritable longKey = new LongWritable();

	@Override
	public void map(LongWritable key, PairedSequence value, Context context)
			throws IOException, InterruptedException {

		longKey.set(value.getKey());
		context.write(longKey, value);
	}
}
