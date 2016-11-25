package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.SequenceFactory;
import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

public class ParseSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private Sequence seq = new Sequence();
	private SeqStringParser parser;
	private short prefixLength;
	private char minQual;
	private boolean isFirstInputPath = true;
	private FileFormat format;

	@Override
	public void setup(Context context) throws IOException {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		format = FileFormat.values()[Integer.parseInt(job.get(Configuration.FILE_FORMAT))];
		prefixLength = Short.parseShort(job.get(Configuration.PREFIXLENGTH));
		minQual = (char) Integer.parseInt(job.get(Configuration.MINQUAL));
		parser = SequenceFactory.createParser(format, prefixLength, minQual);
		String inputPath1 = job.get(Configuration.INPUT_PATH_1);
		String inputSplitPath = ((FileSplit) context.getInputSplit()).getPath().toString();

		if (!inputSplitPath.equals(inputPath1))
			isFirstInputPath = false;
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.toString(), seq) != 0) {
			seq.setIsFirstSeq(isFirstInputPath);
			context.write(key, seq);
		}
	}
}
