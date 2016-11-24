package es.udc.gac.pardre.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SeqStringParser;
import es.udc.gac.pardre.util.SequenceFactory.FileFormat;
import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.SequenceFactory;
import es.udc.gac.pardre.util.Timer;

public class ParDReMR {

	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_PREPARE_PSEQ = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_PRINT_SEQ = 4;

	public static class ParDReMRSingleEndMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

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

	public static class ParDReMRSingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

		private static final NullWritable nullKey = NullWritable.get();
		private short missMatch;

		@Override
		public void setup(Context context) {
			org.apache.hadoop.conf.Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(Configuration.MISSMATCH));
		}

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			boolean insert;
			List<Sequence> sequences = new ArrayList<Sequence>();

			for (Sequence new_seq : values) {
				insert = true;
				
				for (int i = 0; i < sequences.size(); i++) {
					if (new_seq.equal(sequences.get(i), missMatch, true)) {
						insert = false;
						break;
					}
				}
				if (insert)
					sequences.add(new Sequence(new_seq));
			}

			System.out.println("Key "+key+", Group size "+sequences.size());

			for (Sequence seq : sequences) {
				context.write(nullKey, seq);
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
			org.apache.hadoop.conf.Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(Configuration.MISSMATCH));
			mos = new MultipleOutputs<NullWritable,Sequence>(context);
		}

		@Override
		public void cleanup(Context context ) throws IOException, InterruptedException {
			mos.close();
		}

		@Override
		public void reduce(LongWritable key, Iterable<PairedSequence> values, Context context)
				throws IOException, InterruptedException {

			boolean insert;
			List<PairedSequence> sequences = new ArrayList<PairedSequence>();

			for (PairedSequence new_seq : values) {
				insert = true;
				
				for (int i = 0; i < sequences.size(); i++) {
					if (new_seq.getFirstSeq().equal(sequences.get(i).getFirstSeq(), missMatch, true)) {
						if (new_seq.getSecondSeq().equal(sequences.get(i).getSecondSeq(), missMatch, true)) {
							insert = false;
							break;
						}
					}
				}
				if (insert)
					sequences.add(new PairedSequence(new_seq.getFirstSeq(), new_seq.getSecondSeq()));
			}

			System.out.println("Key "+key+", Group size "+sequences.size());

			for (PairedSequence seq : sequences) {
				context.write(nullKey, seq.getFirstSeq());
				mos.write("output2", nullKey, seq.getSecondSeq(), Configuration.TEMP_OUTPUT_PATH_2+"/part");
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

	public static class ParDReMRJoinPairedSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

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

	public static class ParDReMRJoinPairedSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

		private PairedSequence pairedSeq = new PairedSequence();
		private LongWritable longKey = new LongWritable();

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException {

			for (Sequence seq : values) {
				Sequence newSeq = new Sequence(seq);

				if (seq.isFirstSeq()) {
					pairedSeq.setFirstSeq(newSeq);
				} else {
					pairedSeq.setSecondSeq(newSeq);
				}
			}
			longKey.set(pairedSeq.getKey());
			context.write(longKey, pairedSeq);
		}
	}

	private static Job configureSingleEndJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {

		Job job = Job.getInstance(conf, "ParDRe-MR_SingleEnd");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
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

	private static Job configurePairedEndJob(org.apache.hadoop.conf.Configuration conf) throws IOException {

		Job job = Job.getInstance(conf, "ParDRe-MR_PairedEnd");
		job.setJarByClass(ParDReMR.class);

		// Output format classes
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper and Input format classes
		if (Configuration.PAIRED_JOIN) {
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

	private static Job configureJoinPairedSequencesJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_JoinPairedSequences");
		job.setJarByClass(ParDReMR.class);

		// This is a map-only job
		job.setNumReduceTasks(0);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (Configuration.COMPRESS_TEMP) {
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

	private static Job configurePairedSequencesJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_PairedSequences");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (Configuration.COMPRESS_TEMP) {
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

	private static FileFormat detectFileFormat(FileSystem fs, Path file) throws IOException {

		BufferedReader reader = null;
		FileFormat format = FileFormat.FILE_FORMAT_UNKNOWN;

		try {
			reader = new BufferedReader(new InputStreamReader(fs.open(file)));
		} catch (FileNotFoundException e) {
			throw e;
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

		System.out.println("ParDRe-MR: starting from "+Configuration.PARDREMR_HOME);

		timer.start(T_INIT);
		Configuration.loadConfiguration();
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Parse command line options
		Options options = new Options();
		if (!options.parse(otherArgs)) {
			options.printUsage(ParDReMR.class.getName());
			return;
		}
		// FOA, detect the file format
		System.out.println("ParDRe-MR: detecting file format from input file "+options.getReadFileName1());
		FileSystem fs = FileSystem.get(conf);
		Path inputPath1 = fs.resolvePath(new Path(options.getReadFileName1()));
		FileFormat format = ParDReMR.detectFileFormat(fs, inputPath1);

		// Set input format class
		Class<? extends TextInputFormat> inputFormatClass = null;

		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			inputFormatClass = FastQInputFormat.class;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			inputFormatClass = FastAInputFormat.class;
		} else {
			System.out.println("ParDRe-MR: Unrecognized file format");
			System.exit(-1);			
		}

		Path outputPath1 = new Path(options.getWriteFileName1());
		Path tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);
		Path outputPath2 = null;
		Path tmpInputPath1 = null;
		Path tmpInputPath2 = null;
		Path tmpOutputPath2 = null;
		Job jobRemoveDuplicated = null;

		// Set parameters to be used for mapper/reducer classes
		conf.setInt(Configuration.PREFIXLENGTH, options.getPrefixLength());
		conf.setInt(Configuration.MINQUAL, options.getMinQual());
		conf.setInt(Configuration.MISSMATCH, options.getMissMatch());
		conf.setInt(Configuration.FILE_FORMAT, format.ordinal());
		conf.set(Configuration.INPUT_PATH_1, inputPath1.toString());
		conf.setInt(Configuration.IO_BUFFER_SIZE_PARAM, Configuration.IO_FILE_BUFFER_SIZE);

		System.out.println("ParDRe-MR: prefix length = "+options.getPrefixLength());
		System.out.println("ParDRe-MR: quality threshold = "+options.getMinQual());
		System.out.println("ParDRe-MR: allowed missmatches = "+options.getMissMatch());
		System.out.println("ParDRe-MR: input buffer size = "+Configuration.IO_FILE_BUFFER_SIZE);

		if (options.isPaired()) {
			System.out.println("ParDRe-MR: using paired-end mode");

			// Paired-end mode reads (and writes) a second file
			Path inputPath2 = fs.resolvePath(new Path(options.getReadFileName2()));
			outputPath2 = new Path(options.getWriteFileName2());
			tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);
			tmpInputPath1 = new Path(Configuration.TEMP_INPUT_PATH_1);
			tmpInputPath2 = new Path(Configuration.TEMP_INPUT_PATH_2);
			System.out.println("ParDRe-MR: input path = "+inputPath1);
			System.out.println("ParDRe-MR: input path = "+inputPath2);

			if (Configuration.COMPRESS_TEMP)
				System.out.println("ParDRe-MR: using Snappy compression");

			// Configure job to remove duplicated reads
			jobRemoveDuplicated = ParDReMR.configurePairedEndJob(conf);
			FileOutputFormat.setOutputPath(jobRemoveDuplicated, tmpOutputPath1);
			// Add additional output for the second file
			MultipleOutputs.addNamedOutput(jobRemoveDuplicated, "output2", TextOutputFormat.class, NullWritable.class, Sequence.class);

			if (Configuration.PAIRED_JOIN) {
				// Paired-end mode using a map-side join needs two map-only jobs to partition both input files
				Job jobPairedSequences1 = ParDReMR.configureJoinPairedSequencesJob(conf, inputFormatClass);
				Job jobPairedSequences2 = ParDReMR.configureJoinPairedSequencesJob(conf, inputFormatClass);
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
				Job jobPairedSequences = ParDReMR.configurePairedSequencesJob(conf, inputFormatClass);
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
			jobRemoveDuplicated = ParDReMR.configureSingleEndJob(conf, inputFormatClass);
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
		}

		if (!status) {
			System.out.println("ParDRe-MR: job failed!");
			System.exit(-1);
		}

		if (Configuration.MERGE) {
			// Merge output files
			System.out.print("ParDRe-MR: merging output files...");
			timer.start(T_PRINT_SEQ);
			FileUtil.copyMerge(fs, tmpOutputPath1, fs, outputPath1, Configuration.DELETE_TEMP, conf, null);
			if (options.isPaired())
				FileUtil.copyMerge(fs, tmpOutputPath2, fs, outputPath2, Configuration.DELETE_TEMP, conf, null);
			timer.stop(T_PRINT_SEQ);
			System.out.println("done");
		} else {
			outputPath1 = tmpOutputPath1;
			outputPath2 = tmpOutputPath2;
		}

		System.out.println("ParDRe-MR: job completed successfully!");
		System.out.println("ParDRe-MR: output path = "+fs.getUri()+outputPath1);

		// Cleanup
		if (options.isPaired()) {
			System.out.println("ParDRe-MR: output path = "+fs.getUri()+outputPath2);
			if (Configuration.DELETE_TEMP) {
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
		if (Configuration.MERGE)
			System.out.format(java.util.Locale.ENGLISH,"\tOutput files have been merged in %.3f seconds\n", timer.read(T_PRINT_SEQ));

		System.exit(0);
	}
}
