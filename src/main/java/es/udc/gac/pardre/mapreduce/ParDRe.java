package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

	public static class ParDReMapper extends Mapper<Object, Text, LongWritable, Sequence> {

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

			System.out.println("ParDReMapper: prefixLength = "+prefixLength+", minQual = "+(int)minQual);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			/*System.out.println("key:");
			System.out.println(key);
			System.out.println("value:");
			System.out.println(value);
			System.out.println("----------------------------");*/

			if (parser.getSeq(value.toString(), seq) != 0) {
				longKey.set(seq.getKey());
				/*System.out.println("WRITING: KEY "+longKey);
				System.out.println("----------------------------");
				System.out.println(longKey + ": " + seq);*/
				context.write(longKey, seq);
			}
		}
	}

	public static class ParDReReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

		private short missMatch;
		private NullWritable nullWritable = NullWritable.get();

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(MISSMATCHPARAM));

			System.out.println("ParDReReducer: missMatch = " + missMatch);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			List<Sequence> sequences = new ArrayList<Sequence>();

			for (Sequence seq : values) {
				sequences.add(new Sequence(seq));
			}

			//System.out.println("READING: KEY " + key + ", Sequences " + sequences.size());
			/*System.out.println("------------------------------------------");
			for (Sequence seq : sequences)
				System.out.println(key + ": " + seq);*/
			
			for (int i = 0; i < sequences.size(); i++) {
				Sequence seqi = sequences.get(i);

				for (int j = i + 1; j < sequences.size(); j++) {
					if (seqi.equal(sequences.get(j), missMatch, true)) {
						// Delete the second one.
						// In the future we must delete the one with the lowest
						// priority
						sequences.remove(j);
						j--;
					}
				}
				/*System.out.println("WRITING: KEY "+key);
				System.out.println("----------------------------");
				System.out.println(key + ": " + seqi);*/
				context.write(nullWritable, seqi);
			}
		}
	}

	private static Job configureJob1(Configuration conf) throws IOException {
		Job job1 = Job.getInstance(conf, "ParDRe_RemoveDuplicate");
		job1.setJarByClass(ParDRe.class);
		job1.setInputFormatClass(FastQInputFormat.class);
		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Sequence.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Sequence.class);
		job1.setMapperClass(ParDReMapper.class);
		job1.setReducerClass(ParDReReducer.class);
		return job1;
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

		Path inputPath = new Path(options.get_readFileName());
		Path tmpPath = new Path(options.get_writeFileName() + ".tmp");
		Path outputPath = new Path(options.get_writeFileName());

		Job job1 = ParDRe.configureJob1(conf);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job1, inputPath);
		}
		FileOutputFormat.setOutputPath(job1, tmpPath);

		timer.start(T_REMOVE_DUP);
		boolean result = job1.waitForCompletion(true);
		timer.stop(T_REMOVE_DUP);

		timer.start(T_PRINT_SEQ);
		FileSystem fs = FileSystem.get(conf);
		System.out.println("Merging output file...");
		result = FileUtil.copyMerge(fs, tmpPath, fs, outputPath, true, conf, null);
		timer.stop(T_PRINT_SEQ);

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
