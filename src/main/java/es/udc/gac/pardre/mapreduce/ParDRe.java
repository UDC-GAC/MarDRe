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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapred.join.TupleWritable;
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

	public static class ParDReSingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

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

			System.out.println("ParDReSingleEndMapper: prefixLength = "+prefixLength+", minQual = "+(int)minQual);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
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

	public static class ParDReIdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			/*System.out.println("key:");
			System.out.println(key);
			System.out.println("value:");
			System.out.println(value);
			System.out.println("----------------------------");*/
			context.write(key, value);
		}
	}

	public static class ParDReSingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

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

	private static Job configureRemoveDuplicateJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe_RemoveDuplicate");
		job.setJarByClass(ParDRe.class);

		// I/O Format classes
		job.setInputFormatClass(FastQInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper and Reducer classes
		job.setMapperClass(ParDReSingleEndMapper.class);
		job.setReducerClass(ParDReSingleEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configureSortByKeyJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe_SortByKey");
		job.setJarByClass(ParDRe.class);

		// I/O Format classes
		job.setInputFormatClass(FastQInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper/Reducer clases (identity)
		job.setMapperClass(ParDReIdentityMapper.class);
		job.setReducerClass(Reducer.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		return job;
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Options options = new Options();
		if (!options.parse(otherArgs)) {
			options.printUsage("ParDRe MapReduce");
			return;
		}

		conf.setInt(PREFIXLENGTHPARAM, options.get_prefixLength());
		conf.setInt(MINQUALPARAM, options.get_minQual());
		conf.setInt(MISSMATCHPARAM, options.get_missMatch());

		Path inputPath1 = new Path(options.get_readFileName1());
		Path outputPath1 = new Path(options.get_writeFileName1());
		Path tmpPath1 = new Path(outputPath1+".tmp");
		Path inputPath2 = null;
		Path outputPath2 = null;
		Job jobSortByKey = null;
		Job jobRemoveDuplicate = null;

		if (options.isPaired()) {
			//Paired-end datasets read (and write) a second file
			inputPath2 = new Path(options.get_readFileName2());
			outputPath2 = new Path(options.get_writeFileName2());
			//Add additional output for the second file
			MultipleOutputs.addNamedOutput(jobRemoveDuplicate, outputPath2+".tmp", TextOutputFormat.class, NullWritable.class, Sequence.class);
			jobSortByKey = ParDRe.configureSortByKeyJob(conf);
			FileInputFormat.addInputPath(jobSortByKey, inputPath1);
			FileInputFormat.addInputPath(jobSortByKey, inputPath2);
			FileOutputFormat.setOutputPath(jobSortByKey, new Path("input.sorted"));
		}

		jobRemoveDuplicate = ParDRe.configureRemoveDuplicateJob(conf);
		FileInputFormat.addInputPath(jobRemoveDuplicate, inputPath1);
		FileOutputFormat.setOutputPath(jobRemoveDuplicate, tmpPath1);
		
		// Run MapReduce job
		timer.start(T_REMOVE_DUP);
		boolean result = jobRemoveDuplicate.waitForCompletion(true);
		timer.stop(T_REMOVE_DUP);

		// Merge output files
		timer.start(T_PRINT_SEQ);
		FileSystem fs = FileSystem.get(conf);
		System.out.println("Merging output files...");
		result = FileUtil.copyMerge(fs, tmpPath1, fs, outputPath1, true, conf, null);
		timer.stop(T_PRINT_SEQ);

		timer.stop(T_TOTAL);

		if (result) {
			System.out.println("ParDRe completed successfully");
			System.out.format(java.util.Locale.ENGLISH,"Duplicates removed in %.3f seconds\n", timer.read(T_REMOVE_DUP));
			System.out.format(java.util.Locale.ENGLISH,"Sequences printed in %.3f seconds\n", timer.read(T_PRINT_SEQ));
			System.out.format(java.util.Locale.ENGLISH,"Total: %.3f seconds\n", timer.read(T_TOTAL));
			System.exit(0);
		}

		System.out.println("ParDRe failed to complete successfully");
		System.exit(-1);
	}
}
