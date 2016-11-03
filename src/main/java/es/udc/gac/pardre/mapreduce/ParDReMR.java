package es.udc.gac.pardre.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

	public enum FileFormat {
		FILE_FORMAT_FASTA, FILE_FORMAT_FASTQ, FILE_FORMAT_UNKNOWN
	}

	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_PREPARE_PSEQ = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_PRINT_SEQ = 4;
	private static final String PREFIXLENGTH = "ParDRe-MR_prefixLength";
	private static final String MINQUAL = "ParDRe-MR_minQual";
	private static final String MISSMATCH = "ParDRe-MR_missMatch";
	private static final String FILE_FORMAT = "ParDRe-MR_fileFormat";
	private static final String INPUT_PATH_1 = "ParDRe-MR_inputPath1";
	private static final String TEMP_INPUT_PATH_1 = "/ParDRe-MR/tmp/input1";
	private static final String TEMP_INPUT_PATH_2 = "/ParDRe-MR/tmp/input2";
	private static final String TEMP_OUTPUT_PATH_1 = "/ParDRe-MR/tmp/output1";
	private static final String TEMP_OUTPUT_PATH_2 = "/ParDRe-MR/tmp/output2";

	public static class ParDReMRSingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

		private Sequence seq = new Sequence();
		private LongWritable longKey = new LongWritable();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;
		private FileFormat format;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			format = FileFormat.values()[Integer.parseInt(job.get(FILE_FORMAT))];
			prefixLength = Short.parseShort(job.get(PREFIXLENGTH));
			minQual = (char) Integer.parseInt(job.get(MINQUAL));
			parser = new SeqStringParser(prefixLength, minQual);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (parser.getSeq(format, value.toString(), seq) != 0) {
				longKey.set(seq.getKey());
				context.write(longKey, seq);
			}
		}
	}

	public static class ParDReMRSingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

		private static final NullWritable nullKey = NullWritable.get();
		private short missMatch;

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

	public static class ParDReMRPairedEndMapper extends Mapper<LongWritable, PairedSequence, LongWritable, PairedSequence> {

		private LongWritable longKey = new LongWritable();

		@Override
		public void map(LongWritable key, PairedSequence value, Context context)
				throws IOException, InterruptedException {

			longKey.set(value.getKey());
			context.write(longKey, value);
		}
	}

	public static class ParDReMRPairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

		private static final NullWritable nullKey = NullWritable.get();
		private MultipleOutputs<NullWritable,Sequence> mos;
		private short missMatch;	

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
				context.write(nullKey, seqi.getFirstSeq());
				mos.write("output2", nullKey, seqi.getSecondSeq(), TEMP_OUTPUT_PATH_2+"/part");
			}
		}
	}

	public static class ParDReMRJoinPairedEndMapper extends Mapper<LongWritable, TupleWritable, LongWritable, PairedSequence> {

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
			Configuration job = context.getConfiguration();
			format = FileFormat.values()[Integer.parseInt(job.get(FILE_FORMAT))];
			prefixLength = Short.parseShort(job.get(PREFIXLENGTH));
			minQual = (char) Integer.parseInt(job.get(MINQUAL));
			parser = new SeqStringParser(prefixLength, minQual);
		}

		@Override
		public void map(LongWritable key, TupleWritable value, Context context)
				throws IOException, InterruptedException {

			if (parser.getSeq(format, value.get(0).toString(), seq1) != 0) {
				if (parser.getSeq(format, value.get(1).toString(), seq2) != 0) {
					pairedSeq.setFirstSeq(seq1);
					pairedSeq.setSecondSeq(seq2);
					longKey.set(pairedSeq.getKey());
					context.write(longKey, pairedSeq);
				}
			}
		}
	}

	public static class ParDReMRJoinPairedSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

		private Sequence seq = new Sequence();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;
		private boolean isFirstInputPath = true;
		private FileFormat format;

		@Override
		public void setup(Context context) throws IOException {
			Configuration job = context.getConfiguration();
			format = FileFormat.values()[Integer.parseInt(job.get(FILE_FORMAT))];
			prefixLength = Short.parseShort(job.get(PREFIXLENGTH));
			minQual = (char) Integer.parseInt(job.get(MINQUAL));
			parser = new SeqStringParser(prefixLength, minQual);
			String inputPath1 = job.get(INPUT_PATH_1);
			String inputSplitPath = ((FileSplit) context.getInputSplit()).getPath().toString();

			if (!inputSplitPath.equals(inputPath1))
				isFirstInputPath = false;
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (parser.getSeq(format, value.toString(), seq) != 0) {
				seq.setIsFirstSeq(isFirstInputPath);
				context.write(key, seq);
			}
		}
	}

	public static class ParDReMRJoinPairedSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

		private PairedSequence pairedSeq = new PairedSequence();
		private LongWritable longKey = new LongWritable();

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			for (Sequence seq : values) {
				if (seq.isFirstSeq()) {
					pairedSeq.setFirstSeq(new Sequence(seq));
				} else {
					pairedSeq.setSecondSeq(new Sequence(seq));
				}
			}
			longKey.set(pairedSeq.getKey());
			context.write(longKey, pairedSeq);
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

		// Output format classes
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper and Input format classes
		if (es.udc.gac.pardre.util.Configuration.PAIRED_JOIN) {
			job.setInputFormatClass(CompositeInputFormat.class);
			job.setMapperClass(ParDReMRJoinPairedEndMapper.class);
		} else {
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(ParDReMRPairedEndMapper.class);
		}

		// Reducer class
		job.setReducerClass(ParDReMRPairedEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PairedSequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configureJoinPairedSequencesJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_JoinPairedSequences");
		job.setJarByClass(ParDReMR.class);

		// This is a map-only job
		job.setNumReduceTasks(0);

		// I/O Format classes
		job.setInputFormatClass(FastQInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (es.udc.gac.pardre.util.Configuration.COMPRESS_TEMP) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		} else {
			FileOutputFormat.setCompressOutput(job, false);
		}

		// Mapper class
		job.setMapperClass(ParDReMRJoinPairedSequencesMapper.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configurePairedSequencesJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_PairedSequences");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(FastQInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (es.udc.gac.pardre.util.Configuration.COMPRESS_TEMP) {
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		} else {
			FileOutputFormat.setCompressOutput(job, false);
		}

		// Mapper/Reducer classes
		job.setMapperClass(ParDReMRJoinPairedSequencesMapper.class);
		job.setReducerClass(ParDReMRJoinPairedSequencesReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PairedSequence.class);
		return job;
	}

	private static FileFormat detectFileFormat(String fileName) throws IOException {

		BufferedReader reader = null;
		FileFormat format = FileFormat.FILE_FORMAT_UNKNOWN;

		try {
			reader = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			System.out.println("ParDRe-MR: Unrecognized file format");
			return format;
		}

		// Read first line
		String str = reader.readLine();

		// Look for the line where the sequence starts
		while ((str != null) && (str.charAt(0) != '@') && (str.charAt(0) != '>')) {
			str = reader.readLine();
		}

		if (str == null) {
			System.out.println("ParDRe-MR: Unrecognized file format");
		} else if (str.charAt(0) == '@') {
			System.out.println("ParDRe-MR: FASTQ format identified");
			format = FileFormat.FILE_FORMAT_FASTQ;
		} else if (str.charAt(0) == '>') {
			System.out.println("ParDRe-MR: FASTA format identified");
			format = FileFormat.FILE_FORMAT_FASTA;        	
		} else {
			System.out.println("ParDRe-MR: Unrecognized file format");
		}

		reader.close();
		return format;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		boolean status = true;
		Timer timer = new Timer();

		timer.start(T_TOTAL);

		System.out.println("ParDRe-MR: starting from "+es.udc.gac.pardre.util.Configuration.PARDREMR_HOME);

		timer.start(T_INIT);
		es.udc.gac.pardre.util.Configuration.loadConfiguration();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Print out the command line
		System.out.print("ParDRe-MR: command line is ");
		for (int i = 0; i < args.length; i++) {
			System.out.print(args[i]+" ");
		}
		System.out.println();

		// Parse command line options
		Options options = new Options();
		if (!options.parse(otherArgs)) {
			options.printUsage(ParDReMR.class.getName());
			return;
		}

		// FOA, detect the file format
		FileSystem fs = FileSystem.get(conf);
		Path inputPath1 = fs.resolvePath(new Path(options.getReadFileName1()));
		System.out.println("ParDRe-MR: detecting file format of "+inputPath1);
		FileFormat format = ParDReMR.detectFileFormat(options.getReadFileName1());

		if (format == FileFormat.FILE_FORMAT_UNKNOWN) {
			System.exit(-1);
		}

		Path outputPath1 = new Path(options.getWriteFileName1());
		Path tmpOutputPath1 = new Path(TEMP_OUTPUT_PATH_1);
		Path outputPath2 = null;
		Path tmpInputPath1 = null;
		Path tmpInputPath2 = null;
		Path tmpOutputPath2 = null;
		Job jobRemoveDuplicated = null;

		// Set parameters to be used for mapper/reducer classes
		conf.setInt(PREFIXLENGTH, options.getPrefixLength());
		conf.setInt(MINQUAL, options.getMinQual());
		conf.setInt(MISSMATCH, options.getMissMatch());
		conf.setInt(FILE_FORMAT, format.ordinal());
		conf.set(INPUT_PATH_1, inputPath1.toString());

		System.out.println("ParDRe-MR: prefix length = "+options.getPrefixLength());
		System.out.println("ParDRe-MR: quality threshold = "+options.getMinQual());
		System.out.println("ParDRe-MR: allowed missmatches = "+options.getMissMatch());

		if (options.isPaired()) {
			System.out.println("ParDRe-MR: using paired-end mode");

			// Paired-end mode reads (and writes) a second file
			Path inputPath2 = fs.resolvePath(new Path(options.getReadFileName2()));
			outputPath2 = new Path(options.getWriteFileName2());
			tmpOutputPath2 = new Path(TEMP_OUTPUT_PATH_2);
			tmpInputPath1 = new Path(TEMP_INPUT_PATH_1);
			tmpInputPath2 = new Path(TEMP_INPUT_PATH_2);
			System.out.println("ParDRe-MR: input path = "+inputPath1);
			System.out.println("ParDRe-MR: input path = "+inputPath2);

			// Configure job to remove duplicated reads
			jobRemoveDuplicated = ParDReMR.configurePairedEndJob(conf);
			FileOutputFormat.setOutputPath(jobRemoveDuplicated, tmpOutputPath1);
			// Add additional output for the second file
			MultipleOutputs.addNamedOutput(jobRemoveDuplicated, "output2", TextOutputFormat.class, NullWritable.class, Sequence.class);

			if (es.udc.gac.pardre.util.Configuration.PAIRED_JOIN) {
				// Paired-end mode using a map-side join needs two map-only jobs to partition both input files
				Job jobPairedSequences1 = ParDReMR.configureJoinPairedSequencesJob(conf);
				Job jobPairedSequences2 = ParDReMR.configureJoinPairedSequencesJob(conf);
				FileInputFormat.addInputPath(jobPairedSequences1, inputPath1);
				FileInputFormat.addInputPath(jobPairedSequences2, inputPath2);
				FileOutputFormat.setOutputPath(jobPairedSequences1, tmpInputPath1);
				FileOutputFormat.setOutputPath(jobPairedSequences2, tmpInputPath2);
				// Map-side inner join of both partitioned files
				String joinExpression = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, tmpInputPath1, tmpInputPath2);
				jobRemoveDuplicated.getConfiguration().set("mapreduce.join.expr", joinExpression);
				timer.stop(T_INIT);

				// Run first job
				System.out.println("ParDRe-MR: using map-side join. Partitioning input paths...");

				timer.start(T_PREPARE_PSEQ);
				if (jobPairedSequences1.waitForCompletion(true)) {
					// Run second job
					if (jobPairedSequences2.waitForCompletion(true)) {
						timer.stop(T_PREPARE_PSEQ);
					} else {
						status = false;
					}
				} else {
					status = false;
				}
			} else {
				// Paired-end mode without using a map-side join only needs one job
				Job jobPairedSequences = ParDReMR.configurePairedSequencesJob(conf);
				FileInputFormat.addInputPath(jobPairedSequences, inputPath1);
				FileInputFormat.addInputPath(jobPairedSequences, inputPath2);
				FileOutputFormat.setOutputPath(jobPairedSequences, tmpInputPath1);
				FileInputFormat.addInputPath(jobRemoveDuplicated, tmpInputPath1);
				timer.stop(T_INIT);

				// Run job
				System.out.println("ParDRe-MR: pairing sequences...");

				timer.start(T_PREPARE_PSEQ);
				status = jobPairedSequences.waitForCompletion(true);
				timer.stop(T_PREPARE_PSEQ);
			}
		} else {
			// Single-end mode reads (and writes) one file
			System.out.println("ParDRe-MR: using single-end mode");
			System.out.println("ParDRe-MR: input path = "+inputPath1);

			// Configure job
			jobRemoveDuplicated = ParDReMR.configureSingleEndJob(conf);
			FileInputFormat.addInputPath(jobRemoveDuplicated, inputPath1);
			FileOutputFormat.setOutputPath(jobRemoveDuplicated, tmpOutputPath1);
			timer.stop(T_INIT);
		}

		if (status) {
			// Run main job 
			System.out.println("ParDRe-MR: removing duplicated reads...");
			timer.start(T_REMOVE_DUP);
			status = jobRemoveDuplicated.waitForCompletion(true);
			timer.stop(T_REMOVE_DUP);

			if (es.udc.gac.pardre.util.Configuration.MERGE) {
				// Merge output files
				timer.start(T_PRINT_SEQ);
				System.out.println("ParDRe-MR: merging output files...");
				status = FileUtil.copyMerge(fs, tmpOutputPath1, fs, outputPath1, es.udc.gac.pardre.util.Configuration.DELETE_TEMP, conf, null);
				if (options.isPaired())
					status = FileUtil.copyMerge(fs, tmpOutputPath2, fs, outputPath2, es.udc.gac.pardre.util.Configuration.DELETE_TEMP, conf, null);
				timer.stop(T_PRINT_SEQ);
			} else {
				outputPath1 = tmpOutputPath1;
				outputPath2 = tmpOutputPath2;
			}

			System.out.println("ParDRe-MR: completed successfully!");
			System.out.println("ParDRe-MR: output path = "+outputPath1);

			// Cleanup
			if (options.isPaired()) {
				System.out.println("ParDRe-MR: output path = "+outputPath2);
				if (es.udc.gac.pardre.util.Configuration.DELETE_TEMP) {
					fs.delete(tmpInputPath1, true);
					fs.delete(tmpInputPath2, true);
				}
			}

			timer.stop(T_TOTAL);

			// Get task counters
			long inputSequences = jobRemoveDuplicated.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			long outputSequences = jobRemoveDuplicated.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

			System.out.format(java.util.Locale.ENGLISH,"ParDRe-MR: non duplicated paired reads %d/%d\n", outputSequences, inputSequences);
			System.out.format(java.util.Locale.ENGLISH,"ParDRe-MR: execution time was %.3f seconds\n", timer.read(T_TOTAL));
			System.out.format(java.util.Locale.ENGLISH,"\tInitialization has taken %.3f seconds\n", timer.read(T_INIT));
			if (options.isPaired())
				System.out.format(java.util.Locale.ENGLISH,"\tPaired sequences have been setup in %.3f seconds\n", timer.read(T_PREPARE_PSEQ));
			System.out.format(java.util.Locale.ENGLISH,"\tDuplicated reads have been removed in %.3f seconds\n", timer.read(T_REMOVE_DUP));
			if (es.udc.gac.pardre.util.Configuration.MERGE)
				System.out.format(java.util.Locale.ENGLISH,"\tOutput files have been merged in %.3f seconds\n", timer.read(T_PRINT_SEQ));

			System.exit(0);
		}

		System.out.println("ParDRe-MR: failed to complete successfully");
		System.exit(-1);
	}
}
