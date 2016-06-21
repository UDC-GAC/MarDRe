package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.Timer;

public class ParDRe {

	private static final int T_TOTAL = 0;
	private static final int T_REMOVE_DUP = 1;
	private static final int T_PRINT_SEQ = 2;
	private static final String PREFIXLENGTHPARAM = "pardre_prefixLength";
	private static final String MINQUALPARAM = "pardre_minQual";
	private static final String MISSMATCHPARAM = "pardre_missMatch";

	public static class ParDReMapper
	extends Mapper<Object, Text, LongWritable, Sequence> {

		private Sequence seq = new Sequence();
		private LongWritable longKey = new LongWritable();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			prefixLength = Short.parseShort(job.get(PREFIXLENGTHPARAM));
			minQual = (char) Integer.parseInt(job.get(MINQUALPARAM));
			parser = new SeqStringParser(prefixLength, minQual);

			System.out.println("ParDreMapper: prefixLength = "+prefixLength+", minQual = "+(int)minQual);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (parser.getSeq(value.toString(), seq) != 0) {
				longKey.set(seq.getKey());
				System.out.println("WRITING " + longKey + ": " + seq.toStringFull());
				context.write(longKey, seq);
			}
		}
	}

	public static class ParDReReducer
	extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

		private short missMatch;
		private NullWritable caca = NullWritable.get();

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(MISSMATCHPARAM));

			System.out.println("ParDreReducer: missMatch = " + missMatch);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			List<Sequence> sequences = new ArrayList<Sequence>();

			for (Sequence seq : values) {
				sequences.add(new Sequence(seq));
			}

			System.out.println("KEY=" + key + " - " + sequences.size());
			for (Sequence seq : sequences) {
				System.out.println(key + ": " + seq);
			}

			for (int i = 0; i < sequences.size(); i++) {
				Sequence seqi = sequences.get(i);

				for (int j = i + 1; j < sequences.size(); j++) {
					if (seqi.equal(sequences.get(j), missMatch, true)) {
						// Delete the second one.
						// In the future we must delete the one with the lowest
						// priority
						System.out.println(seqi.toStringFull() + " equals to <"
								+ sequences.get(j).toStringFull());
						System.out.println(
								"REMOVING " + key + ": " + sequences.get(j));
						sequences.remove(j);
						j--;
					}
				}
				System.out.println("WRITING " + key + ": " + seqi);
				context.write(caca, seqi);
			}
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Timer timer = new Timer();

		System.out.println("Starting ParDRe MapReduce");

		timer.start(T_TOTAL);

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Options options = new Options();
		if (!options.parse(otherArgs)) {
			options.printUsage("ParDRe MapReduce");
			return;
		}

		conf.setInt(PREFIXLENGTHPARAM, options.get_prefixLength());
		conf.setInt(MINQUALPARAM, options.get_minQual());
		conf.setInt(MISSMATCHPARAM, options.get_missMatch());
		conf.set("textinputformat.record.delimiter", "@");

		Path inputPath = new Path(options.get_readFileName());
		Path tmpPath = new Path(options.get_writeFileName() + ".tmp");
		Path outputPath = new Path(options.get_writeFileName());

		Job job1 = Job.getInstance(conf, "ParDRe_RemoveDuplicate");
		job1.setJarByClass(ParDRe.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Sequence.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Sequence.class);
		job1.setMapperClass(ParDReMapper.class);
		job1.setReducerClass(ParDReReducer.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job1, inputPath);
		}
		FileOutputFormat.setOutputPath(job1, tmpPath);

		timer.start(T_REMOVE_DUP);
		boolean result = job1.waitForCompletion(true);
		timer.stop(T_REMOVE_DUP);

		Job job2 = Job.getInstance(conf, "ParDRe_SaveOutput");
		job2.setJarByClass(ParDRe.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Sequence.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Sequence.class);
		job2.setMapperClass(Mapper.class);
		job2.setReducerClass(Reducer.class);
		job2.setNumReduceTasks(1);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job2, tmpPath);
		}
		FileOutputFormat.setOutputPath(job2, outputPath);

		timer.start(T_PRINT_SEQ);
		result = job2.waitForCompletion(true);
		timer.stop(T_PRINT_SEQ);

		tmpPath.getFileSystem(conf).delete(tmpPath, true);

		timer.stop(T_TOTAL);

		if (result) {
			System.out.println("Application completed successfully");
			System.out.format(java.util.Locale.ENGLISH,"Remove duplicate in %.3f seconds\n", timer.read(T_REMOVE_DUP));
			System.out.format(java.util.Locale.ENGLISH,"Print sequences in %.3f seconds\n", timer.read(T_PRINT_SEQ));
			System.out.format(java.util.Locale.ENGLISH,"Done in %.3f seconds\n", timer.read(T_TOTAL));
			System.exit(0);
		}

		System.out.println("Application failed to complete successfully");
		System.exit(-1);
	}
}
