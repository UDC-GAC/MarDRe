package es.udc.gac.pardre.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.Options;
import es.udc.gac.pardre.util.SequenceFactory.FileFormat;
import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;
import es.udc.gac.pardre.util.Timer;

public class ParDReMR {

	// Timers
	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_PREPARE_PSEQ = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_PRINT_SEQ = 4;

	private static Job configureSingleEndJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {

		Job job = Job.getInstance(conf, "ParDRe-MR_SingleEnd");
		job.setJarByClass(ParDReMR.class);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper and Reducer classes
		job.setMapperClass(SingleEndMapper.class);
		job.setReducerClass(SingleEndReducer.class);

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
			job.setMapperClass(JoinPairedEndMapper.class);
		} else {
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(PairedEndMapper.class);
		}

		// Reducer class
		job.setReducerClass(PairedEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PairedSequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configureHadoopJoinSequencesJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_HadoopJoinSequences");
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

		// Mapper class
		job.setMapperClass(ParseSequencesMapper.class);

		// This is a map-only job
		job.setNumReduceTasks(0);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configureJoinSequencesJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {
		Job job = Job.getInstance(conf, "ParDRe-MR_JoinSequences");
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
		job.setMapperClass(ParseSequencesMapper.class);
		job.setReducerClass(GroupSequencesReducer.class);

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
		conf.setBoolean(Configuration.IN_MAPPER_COMBINER_PARAM, Configuration.IN_MAPPER_COMBINER);
		conf.setInt(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE_PARAM, Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);

		System.out.println("ParDRe-MR: prefix length = "+options.getPrefixLength());
		System.out.println("ParDRe-MR: quality threshold = "+options.getMinQual());
		System.out.println("ParDRe-MR: allowed missmatches = "+options.getMissMatch());
		System.out.println("ParDRe-MR: input buffer size = "+Configuration.IO_FILE_BUFFER_SIZE);
		System.out.println("ParDRe-MR: in-Mapper combiner = "+Configuration.IN_MAPPER_COMBINER);
		System.out.println("ParDRe-MR: in-Mapper combiner cache size = "+Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);

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
				// Paired-end mode using a map-side Hadooop join needs two map-only jobs to partition both input files
				Job jobPairedSequences1 = ParDReMR.configureHadoopJoinSequencesJob(conf, inputFormatClass);
				Job jobPairedSequences2 = ParDReMR.configureHadoopJoinSequencesJob(conf, inputFormatClass);
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
				// Paired-end mode using a manual join only needs one job processing both input files
				Job jobPairedSequences = ParDReMR.configureJoinSequencesJob(conf, inputFormatClass);
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
			System.out.format(java.util.Locale.ENGLISH,"\tSequences have been paired in %.3f seconds\n", timer.read(T_PREPARE_PSEQ));
		System.out.format(java.util.Locale.ENGLISH,"\tDuplicated reads have been removed in %.3f seconds\n", timer.read(T_REMOVE_DUP));
		if (Configuration.MERGE)
			System.out.format(java.util.Locale.ENGLISH,"\tOutput files have been merged in %.3f seconds\n", timer.read(T_PRINT_SEQ));

		System.exit(0);
	}
}
