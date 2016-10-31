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
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.Timer;

public class ParDReMR {

	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_REMOVE_DUP = 2;
	private static final int T_PRINT_SEQ = 3;
	private static final String PREFIXLENGTH = "ParDRe-MR_prefixLength";
	private static final String MINQUAL = "ParDRe-MR_minQual";
	private static final String MISSMATCH = "ParDRe-MR_missMatch";
	private static final String TEMP_PATH_1 = "/ParDRe-MR/tmp/output1";
	private static final String TEMP_PATH_2 = "/ParDRe-MR/tmp/output2";

	public static class ParDReMRSingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

		private Sequence seq = new Sequence();
		private LongWritable longKey = new LongWritable();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			prefixLength = Short.parseShort(job.get(PREFIXLENGTH));
			minQual = (char) Integer.parseInt(job.get(MINQUAL));
			parser = new SeqStringParser(prefixLength, minQual);
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

	public static class ParDReMRPairedEndMapper extends Mapper<LongWritable, TupleWritable, LongWritable, PairedSequence> {

		private PairedSequence pairedSeq = new PairedSequence();
		private Sequence seq1 = new Sequence();
		private Sequence seq2 = new Sequence();
		private LongWritable longKey = new LongWritable();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			prefixLength = Short.parseShort(job.get(PREFIXLENGTH));
			minQual = (char) Integer.parseInt(job.get(MINQUAL));
			parser = new SeqStringParser(prefixLength, minQual);
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

	public static class ParDReMRPairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

		private short missMatch;	
		private static final NullWritable nullKey = NullWritable.get();
		private MultipleOutputs<NullWritable,Sequence> mos;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(MISSMATCH));
			mos = new MultipleOutputs<NullWritable,Sequence>(context);
		}

		@Override
		public void cleanup(Context context ) throws IOException, InterruptedException {
			mos.close();
		}

		@Override
		public void reduce(LongWritable key, Iterable<PairedSequence> values, Context context)
				throws IOException, InterruptedException {

			List<PairedSequence> sequences = new ArrayList<PairedSequence>();

			for (PairedSequence seq : values) {
				sequences.add(new PairedSequence(seq.getFirstSeq(), seq.getSecondSeq()));
			}

			for (int i = 0; i < sequences.size(); i++) {
				PairedSequence seqi = sequences.get(i);

				for (int j = i + 1; j < sequences.size(); j++) {
					if (seqi.getFirstSeq().equal(sequences.get(j).getFirstSeq(), missMatch, true)) {
						if (seqi.getSecondSeq().equal(sequences.get(j).getSecondSeq(), missMatch, true)) {
							// Delete the second one.
							// In the future we must delete the one with the lowest
							// priority
							sequences.remove(j);
							j--;
						}
					}
				}
				mos.write("output2", nullKey, seqi.getSecondSeq(), TEMP_PATH_2+"/part");
				context.write(nullKey, seqi.getFirstSeq());
			}
		}
	}

	public static class ParDReMRSingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

		private short missMatch;
		private static final NullWritable nullKey = NullWritable.get();

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(MISSMATCH));
		}

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			List<Sequence> sequences = new ArrayList<Sequence>();

			for (Sequence seq : values) {
				sequences.add(new Sequence(seq));
			}

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
				context.write(nullKey, seqi);
			}
		}
	}

	private static Job configureSingleEndJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_SingleEnd");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(FastQInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper and Reducer classes
		job.setMapperClass(ParDReMRSingleEndMapper.class);
		job.setReducerClass(ParDReMRSingleEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configurePairedEndJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_PairedEnd");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(CompositeInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper/Reducer clases (identity)
		job.setMapperClass(ParDReMRPairedEndMapper.class);
		job.setReducerClass(ParDReMRPairedEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PairedSequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Timer timer = new Timer();

		System.out.println("Starting ParDRe-MR ("+es.udc.gac.pardre.util.Configuration.PARDREMR_HOME+")");

		timer.start(T_TOTAL);

		timer.start(T_INIT);
		es.udc.gac.pardre.util.Configuration.loadConfiguration();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Options options = new Options();
		if (!options.parse(otherArgs)) {
			options.printUsage("ParDRe-MR");
			return;
		}

		conf.setInt(PREFIXLENGTH, options.get_prefixLength());
		conf.setInt(MINQUAL, options.get_minQual());
		conf.setInt(MISSMATCH, options.get_missMatch());

		Path inputPath1 = new Path(options.get_readFileName1());
		Path outputPath1 = new Path(options.get_writeFileName1());
		Path outputPath2 = null;
		Path tmpPath1 = new Path(TEMP_PATH_1);
		Path tmpPath2 = null;
		Job jobRemoveDuplicate = null;

		if (options.isPaired()) {
			System.out.println("Using paired-end mode");
			// Paired-end datasets read (and write) a second file
			Path inputPath2 = new Path(options.get_readFileName2());
			outputPath2 = new Path(options.get_writeFileName2());
			tmpPath2 = new Path(TEMP_PATH_2);

			// Configure job
			jobRemoveDuplicate = ParDReMR.configurePairedEndJob(conf);
			FileOutputFormat.setOutputPath(jobRemoveDuplicate, tmpPath1);
			// Add additional output for the second file
			MultipleOutputs.addNamedOutput(jobRemoveDuplicate, "output2", TextOutputFormat.class, NullWritable.class, Sequence.class);

			// Inner join of both input files
			String joinExpression = CompositeInputFormat.compose("inner", FastQInputFormat.class, inputPath1, inputPath2);
			jobRemoveDuplicate.getConfiguration().set("mapreduce.join.expr", joinExpression);
		} else {
			System.out.println("Using single-end mode");
			// Configure job
			jobRemoveDuplicate = ParDReMR.configureSingleEndJob(conf);
			FileInputFormat.addInputPath(jobRemoveDuplicate, inputPath1);
			FileOutputFormat.setOutputPath(jobRemoveDuplicate, tmpPath1);
		}
		timer.stop(T_INIT);

		System.out.format(java.util.Locale.ENGLISH,"ParDRe-MR initialized in %.3f seconds\n", timer.read(T_INIT));

		// Run MapReduce job
		timer.start(T_REMOVE_DUP);
		boolean status = jobRemoveDuplicate.waitForCompletion(true);
		timer.stop(T_REMOVE_DUP);

		if (status) {
			if (es.udc.gac.pardre.util.Configuration.MERGE) {
				// Merge output files
				timer.start(T_PRINT_SEQ);
				FileSystem fs = FileSystem.get(conf);
				System.out.println("Merging output files...");
				status = FileUtil.copyMerge(fs, tmpPath1, fs, outputPath1, es.udc.gac.pardre.util.Configuration.DELETE_TEMP, conf, null);
				if (options.isPaired())
					status = FileUtil.copyMerge(fs, tmpPath2, fs, outputPath2, es.udc.gac.pardre.util.Configuration.DELETE_TEMP, conf, null);
				timer.stop(T_PRINT_SEQ);
			}

			timer.stop(T_TOTAL);

			System.out.println("ParDRe-MR completed successfully");
			System.out.format(java.util.Locale.ENGLISH,"Initialization: %.3f seconds\n", timer.read(T_INIT));
			System.out.format(java.util.Locale.ENGLISH,"Duplicates have been removed in %.3f seconds\n", timer.read(T_REMOVE_DUP));
			if (es.udc.gac.pardre.util.Configuration.MERGE)
				System.out.format(java.util.Locale.ENGLISH,"Sequences have been merged in %.3f seconds\n", timer.read(T_PRINT_SEQ));
			System.out.format(java.util.Locale.ENGLISH,"Total: %.3f seconds\n", timer.read(T_TOTAL));
			System.exit(0);
		}

		System.out.println("ParDRe-MR failed to complete successfully");
		System.exit(-1);
	}
}
