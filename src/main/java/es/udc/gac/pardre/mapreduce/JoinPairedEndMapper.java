package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

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
	public void map(LongWritable key, TupleWritable value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.get(0).toString(), seq1) != 0) {
			if (parser.getSeq(value.get(1).toString(), seq2) != 0) {
				pairedSeq.setFirstSeq(seq1);
				pairedSeq.setSecondSeq(seq2);
				longKey.set(pairedSeq.getKey());
				context.write(longKey, pairedSeq);
			}
		}
	}
}
