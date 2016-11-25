package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

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
	private FileFormat format;

	@Override
	public void setup(Context context) {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		format = FileFormat.values()[Integer.parseInt(job.get(Configuration.FILE_FORMAT))];
		prefixLength = Short.parseShort(job.get(Configuration.PREFIXLENGTH));
		minQual = (char) Integer.parseInt(job.get(Configuration.MINQUAL));
		parser = SequenceFactory.createParser(format, prefixLength, minQual);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.toString(), seq) != 0) {
			longKey.set(seq.getKey());
			context.write(longKey, seq);
		}
	}
}
