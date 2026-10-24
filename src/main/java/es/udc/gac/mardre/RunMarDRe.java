/*
 * Copyright (C) 2017-2026 Universidade da Coruña
 * 
 * This file is part of MarDRe.
 * 
 * MarDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MarDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;
import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.Errors;
import es.udc.gac.mardre.util.LongKeyComparator;
import es.udc.gac.mardre.util.Options;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;
import es.udc.gac.mardre.util.SequenceParserFactory;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class RunMarDRe extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(RunMarDRe.class);

	// Timers
	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_PARSE_READS = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_MERGE = 4;

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new RunMarDRe(), args));
	}

	private static Job configureSingleEndJob(org.apache.hadoop.conf.Configuration conf, 
			Class<? extends SingleEndSequenceInputFormat> inputFormatClass, 
			Path inputPath, Path outputPath, int nreducers, CompressionCodec codec) throws IOException {

		Job job = Job.getInstance(conf, "MarDRe_SingleEnd");
		job.setJarByClass(RunMarDRe.class);

		// Number of reducers
		job.setNumReduceTasks(nreducers);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(TextOutputFormat.class);

		// I/O paths
		SingleEndSequenceInputFormat.addInputPath(job, inputPath);
		TextOutputFormat.setOutputPath(job, outputPath);

		// Compress output
		if (codec != null) {
			TextOutputFormat.setCompressOutput(job, true);
			TextOutputFormat.setOutputCompressorClass(job, codec.getClass());
		} else {
			TextOutputFormat.setCompressOutput(job, false);
		}

		// Mapper and Reducer classes
		job.setMapperClass(SingleEndMapper.class);
		job.setReducerClass(SingleEndReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setSortComparatorClass(LongKeyComparator.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configurePairedEndJob(org.apache.hadoop.conf.Configuration conf,
			Class<? extends SingleEndSequenceInputFormat> inputFormatClass,
			Path inputPath1, Path inputPath2, Path outputPath,
			int nreducers, CompressionCodec codec) throws IOException {

		Job job = Job.getInstance(conf, "MarDRe_PairedEnd");
		job.setJarByClass(RunMarDRe.class);

		// Number of reducers
		job.setNumReduceTasks(nreducers);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

		// Mapper and Input format classes
		if (Configuration.PAIRED_END_MAP_JOIN) {
			// Input format class
			job.setInputFormatClass(PairedEndSequenceInputFormat.class);

			// Set left and right input paths
			PairedEndSequenceInputFormat.setLeftInputPath(job, inputPath1, inputFormatClass);
			PairedEndSequenceInputFormat.setRightInputPath(job, inputPath2, inputFormatClass);

			// Mapper class
			job.setMapperClass(PairedEndMapJoinMapper.class);
		} else {
			// Input format class
			job.setInputFormatClass(SequenceFileInputFormat.class);

			// Set input path
			SequenceFileInputFormat.addInputPath(job, inputPath1);

			// Mapper class
			job.setMapperClass(PairedEndReduceJoinMapper.class);
		}

		// Reducer class
		job.setReducerClass(PairedEndReducer.class);

		// Output format class
		job.setOutputFormatClass(TextOutputFormat.class);

		// Output path
		TextOutputFormat.setOutputPath(job, outputPath);
		// Add additional output for the second file
		MultipleOutputs.addNamedOutput(job, "output2", TextOutputFormat.class, NullWritable.class, Sequence.class);

		// Compress output
		if (codec != null) {
			TextOutputFormat.setCompressOutput(job, true);
			TextOutputFormat.setOutputCompressorClass(job, codec.getClass());
		} else {
			TextOutputFormat.setCompressOutput(job, false);
		}

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setSortComparatorClass(LongKeyComparator.class);
		job.setMapOutputValueClass(PairedSequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sequence.class);
		return job;
	}

	private static Job configureReduceJoinSequencesJob(org.apache.hadoop.conf.Configuration conf,
			Class<? extends SingleEndSequenceInputFormat> inputFormatClass,
			Path inputPath1, Path inputPath2, Path outputPath, int nreducers) throws IOException {

		Job job = Job.getInstance(conf, "MarDRe_ReduceJoinSequences");
		job.setJarByClass(RunMarDRe.class);

		// Number of reducers
		job.setNumReduceTasks(nreducers);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// I/O paths
		SingleEndSequenceInputFormat.setInputPaths(job, inputPath1, inputPath2);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		// Compress output
		if (Configuration.COMPRESS_INTERMEDIATE_OUTPUT) {
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		} else {
			SequenceFileOutputFormat.setCompressOutput(job, false);
		}

		// Mapper/Reducer classes
		job.setMapperClass(ParseSequencesMapper.class);
		job.setReducerClass(GroupSequencesReducer.class);

		// Mapper output K/V classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setSortComparatorClass(LongKeyComparator.class);
		job.setMapOutputValueClass(Sequence.class);

		// Job ouput K/V classes
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PairedSequence.class);
		return job;
	}

	private static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int bufferSize, short replication,
			long blockSize, boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			throw new IOException("Input path is not a directory");

		FSDataOutputStream out = dstFS.create(dstFile, true, bufferSize, replication, blockSize);
		FSDataInputStream in = null;

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			Arrays.sort(contents);
			for (int i = 0; i < contents.length; i++) {
				if (contents[i].isFile()) {
					in = srcFS.open(contents[i].getPath());
					try {
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					} 
				}
			}
		} finally {
			out.close();
		}

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	private static boolean isInputPathCompressed(org.apache.hadoop.conf.Configuration conf, Path inputPath) {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);

		if (codec == null)
			return false;

		return true;
	}

	@Override
	public int run(String[] args) throws Exception {

		Job jobRemoveDuplicate = null;
		Path inputPath1 = null, inputPath2 = null;
		Path tmpInputPath1 = null;
		Path outputPath1 = null, outputPath2 = null;
		Path tmpOutputPath1 = null, tmpOutputPath2 = null;
		CompressionCodec codec = null;
		long inputPath1Length = 0, inputPath2Length = 0;
		long inputPath1LengthMB = 0, inputPath2LengthMB = 0;
		boolean isInputPath1Compressed = false, isInputPath2Compressed = false;
		boolean status = true;

		timer.start(T_TOTAL);

		timer.start(T_INIT);

		LOG.info("Starting from MARDRE_HOME = {}", Configuration.getHome());

		// Get Hadoop configuration
		org.apache.hadoop.conf.Configuration conf = this.getConf();

		// Parse command line options
		Options options = new Options();
		if (!options.parse(args))
			return -1;

		// Get input files and check if they are compressed
		FileSystem fs = FileSystem.get(conf);
		inputPath1 = fs.resolvePath(new Path(options.getReadFileName1()));
		inputPath1Length = fs.getFileStatus(inputPath1).getLen();
		inputPath1LengthMB = inputPath1Length / (1024 * 1024);
		isInputPath1Compressed = RunMarDRe.isInputPathCompressed(conf, inputPath1);
		long inputPathBlockSize = fs.getFileStatus(inputPath1).getBlockSize();
		long inputPathBlockSizeMB = inputPathBlockSize / (1024 * 1024);

		if (isInputPath1Compressed)
			// If codec != null => Output must be compressed
			codec = new CompressionCodecFactory(conf).getCodec(inputPath1);

		if (options.isPaired()) {
			inputPath2 = fs.resolvePath(new Path(options.getReadFileName2()));
			inputPath2Length = fs.getFileStatus(inputPath2).getLen();
			inputPath2LengthMB = inputPath2Length / (1024 * 1024);
			isInputPath2Compressed = RunMarDRe.isInputPathCompressed(conf, inputPath2);		
		}

		// Determine the file format
		FileFormat format = options.getInputFileFormat();

		if (format == FileFormat.FILE_FORMAT_UNKNOWN) {
			// Try to autodetect the file format
			LOG.info("Detecting input file format from {}", inputPath1);

			if (isInputPath1Compressed)
				throw new IOException("Input file "+inputPath1.getName()+" is compressed. You must specify its format using -q (FASTQ) or -f (FASTA) command-line options.");

			format = SequenceParserFactory.autoDetectFileFormat(fs, inputPath1, options.getPrefixLength());
		}

		// Set input format class
		Class<? extends SingleEndSequenceInputFormat> inputFormatClass = null;

		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			LOG.info("FASTQ format identified");
			inputFormatClass = FastQInputFormat.class;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			LOG.info("FASTA format identified");
			inputFormatClass = FastAInputFormat.class;
		}

		// Load MarDRe configuration
		if (!Configuration.load(fs, conf, options, inputPath1, format))
			return -1;

		// Get output paths
		outputPath1 = new Path(Configuration.OUTPUT_PATH + "/" + (new Path(options.getWriteFileName1()).getName()));
		tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);

		if (fs.exists(tmpOutputPath1))
			fs.delete(tmpOutputPath1, true);

		// Print command line options and configuration parameters
		LOG.info("prefix length = {}", options.getPrefixLength());
		LOG.info("allowed mismatches = {}", options.getMisMatch());
		LOG.info("number of bases to compare = {}", options.getNumberOfBasesCompared());
		if (options.getOpticalDist() > 0)
			LOG.info("only optical duplicates, at most {} apart", options.getOpticalDist());
		LOG.info("number of reducers = {}", options.getNumberOfReducers());
		LOG.info("input buffer size = {} bytes", Configuration.INPUT_BUFFER_SIZE);
		LOG.info("in-Mapper combiner = {}", Configuration.IN_MAPPER_COMBINER);
		LOG.info("in-Mapper combiner cache = {} entries", Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);
		LOG.info("cluster initial capacity = {} entries", Configuration.CLUSTER_LIST_INITIAL_CAPACITY);
		LOG.info("HDFS base path = {}", Configuration.HDFS_BASE_PATH);
		LOG.info("HDFS block replication = {}", Configuration.HDFS_BLOCK_REPLICATION);
		LOG.info("HDFS block size = {} MiBytes", inputPathBlockSizeMB);

		// Check compression options
		if (Configuration.COMPRESS_MAP_OUTPUT || Configuration.COMPRESS_INTERMEDIATE_OUTPUT) {
			try {
				// Hadoop 3.3 has no native code check for Snappy: creating a compressor fails if it is not usable
				ReflectionUtils.newInstance(SnappyCodec.class, conf).createCompressor();
			} catch (Exception | LinkageError e) {
				LOG.warn("Snappy compression of the map output is not available: {}", e.getMessage());
				Configuration.COMPRESS_MAP_OUTPUT = false;
			}
			conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, Configuration.COMPRESS_MAP_OUTPUT);
			conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
		}

		if (Configuration.COMPRESS_MAP_OUTPUT)
			LOG.info("using map output Snappy compression");

		// Configure some MapReduce/HDFS properties
		conf.setInt(MRJobConfig.TASK_TIMEOUT, 0);
		conf.setFloat(MRJobConfig.MAP_SORT_SPILL_PERCENT, 0.9f);
		// Force one block replica for intermediate output files
		conf.setInt("dfs.replication", 1);
		// The default of HDFS if its configuration is not loaded; it may have a suffix (128m)
		long blockSize = conf.getLongBytes("dfs.blocksize", 128L * 1024 * 1024);

		if (options.isPaired()) {
			LOG.info("running in paired-end mode");

			if (Configuration.COMPRESS_INTERMEDIATE_OUTPUT)
				LOG.info("using intermediate output Snappy compression");

			// Paired-end mode writes a second output file
			outputPath2 = new Path(Configuration.OUTPUT_PATH + "/" + (new Path(options.getWriteFileName2()).getName()));
			tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);
			tmpInputPath1 = new Path(Configuration.TEMP_INPUT_PATH_1);

			if (inputPath1LengthMB > 0)
				LOG.info("input path = {} ({} MiBytes)", inputPath1, inputPath1LengthMB);
			else
				LOG.info("input path = {} ({} bytes)", inputPath1, inputPath1Length);

			if (inputPath2LengthMB > 0)
				LOG.info("input path = {} ({} MiBytes)", inputPath2, inputPath2LengthMB);
			else
				LOG.info("input path = {} ({} bytes)", inputPath2, inputPath2Length);

			if ((isInputPath1Compressed && !isInputPath2Compressed) || (!isInputPath1Compressed && isInputPath2Compressed))
				throw new UnsupportedOperationException("Both input files must be compressed or not compressed");

			if (!isInputPath1Compressed && !isInputPath2Compressed) {
				if (inputPath1Length != inputPath2Length)
					throw new IOException("Input files must be the same size in paired-end mode");
			}

			if (fs.exists(tmpInputPath1))
				fs.delete(tmpInputPath1, true);
			if (fs.exists(tmpOutputPath2))
				fs.delete(tmpOutputPath2, true);

			if (Configuration.PAIRED_END_MAP_JOIN) {
				/*
				 * Paired-end mode using a map-side join does not need any previous preprocessing
				 * job. Just configure duplicate removal job
				 */ 
				jobRemoveDuplicate = RunMarDRe.configurePairedEndJob(conf, inputFormatClass, inputPath1, inputPath2, tmpOutputPath1, options.getNumberOfReducers(), codec);
				timer.stop(T_INIT);
			} else {
				/* 
				 * Paired-end mode using a reduce-side join first executes a map-reduce job in order
				 * to preprocess both input files
				 */
				// Configure preprocessing job
				Job jobPairedSequences = RunMarDRe.configureReduceJoinSequencesJob(conf, inputFormatClass, inputPath1, inputPath2, tmpInputPath1, options.getNumberOfReducers());
				// Configure duplicate removal job 
				jobRemoveDuplicate = RunMarDRe.configurePairedEndJob(conf, inputFormatClass, tmpInputPath1, null, tmpOutputPath1, options.getNumberOfReducers(), codec);
				timer.stop(T_INIT);

				// Run preprocessing job
				LOG.info("using reduce-side join. Joining paired sequences...");
				timer.start(T_PARSE_READS);
				status = jobPairedSequences.waitForCompletion(true);
				timer.stop(T_PARSE_READS);

				if (!status) {
					Errors.report(LOG, "Job {} failed: {}", jobPairedSequences.getJobID(), jobPairedSequences.getStatus().getFailureInfo());
					Errors.reportTaskFailures(LOG, jobPairedSequences);
					return -1;
				} else {
					Path success = new Path(tmpInputPath1+"/_SUCCESS");

					if (fs.exists(success))
						fs.delete(success, true);
				}
			}
		} else {
			// Single-end mode reads and writes a single file
			LOG.info("running in single-end mode");

			if (inputPath1LengthMB > 0)
				LOG.info("input path = {} ({} MiBytes)", inputPath1, inputPath1LengthMB);
			else
				LOG.info("input path = {} ({} bytes)", inputPath1, inputPath1Length);

			// Configure job to remove duplicate reads
			jobRemoveDuplicate = RunMarDRe.configureSingleEndJob(conf, inputFormatClass, inputPath1, tmpOutputPath1, options.getNumberOfReducers(), codec);
			timer.stop(T_INIT);
		}

		// Run duplicate removal job 
		LOG.info("removing duplicate reads...");
		timer.start(T_REMOVE_DUP);
		status = jobRemoveDuplicate.waitForCompletion(true);
		timer.stop(T_REMOVE_DUP);

		if (!status) {
			Errors.report(LOG, "Job {} failed: {}", jobRemoveDuplicate.getJobID(), jobRemoveDuplicate.getStatus().getFailureInfo());
			Errors.reportTaskFailures(LOG, jobRemoveDuplicate);
			return -1;
		}

		Path success = new Path(tmpOutputPath1+"/_SUCCESS");

		if (fs.exists(success))
			fs.delete(success, true);

		if (Configuration.MERGE) {
			// Merge output files
			LOG.info("merging output files...");
			timer.start(T_MERGE);
			RunMarDRe.merge(fs, tmpOutputPath1, fs, outputPath1, Configuration.INPUT_BUFFER_SIZE, Configuration.HDFS_BLOCK_REPLICATION, blockSize, Configuration.DELETE_TEMP, conf);
			if (options.isPaired())
				RunMarDRe.merge(fs, tmpOutputPath2, fs, outputPath2, Configuration.INPUT_BUFFER_SIZE, Configuration.HDFS_BLOCK_REPLICATION, blockSize, Configuration.DELETE_TEMP, conf);
			timer.stop(T_MERGE);
			LOG.info("output files merged");
		} else {
			outputPath1 = tmpOutputPath1;
			outputPath2 = tmpOutputPath2;
		}

		System.out.println("MarDRe: output path = "+outputPath1);

		// Cleanup
		if (options.isPaired()) {
			System.out.println("MarDRe: output path = "+outputPath2);
			if (Configuration.DELETE_TEMP)
				fs.delete(tmpInputPath1, true);
		}

		timer.stop(T_TOTAL);

		// Get task counters
		long inputSequences = jobRemoveDuplicate.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		long outputSequences = jobRemoveDuplicate.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

		float percent = (float) outputSequences;
		percent /= (float) inputSequences;
		percent *= 100.00;

		System.out.format(java.util.Locale.ENGLISH,"MarDRe: non duplicate reads %d/%d (%.2f%%)\n", outputSequences, inputSequences, percent);
		System.out.format(java.util.Locale.ENGLISH,"MarDRe: execution time was %.3f seconds\n", timer.readTotal(T_TOTAL));
		System.out.format(java.util.Locale.ENGLISH,"\tInitialization took %.3f seconds\n", timer.readTotal(T_INIT));
		if (options.isPaired() && !Configuration.PAIRED_END_MAP_JOIN)
			System.out.format(java.util.Locale.ENGLISH,"\tSequences were paired in %.3f seconds\n", timer.readTotal(T_PARSE_READS));
		System.out.format(java.util.Locale.ENGLISH,"\tDuplicate reads were removed in %.3f seconds\n", timer.readTotal(T_REMOVE_DUP));
		if (Configuration.MERGE)
			System.out.format(java.util.Locale.ENGLISH,"\tOutput files were merged in %.3f seconds\n", timer.readTotal(T_MERGE));

		return 0;
	}
}
