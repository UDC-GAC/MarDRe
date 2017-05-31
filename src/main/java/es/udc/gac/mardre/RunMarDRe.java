/*
 * Copyright (C) 2017 Universidade da Coruña
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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.Options;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.Timer;
import es.udc.gac.mardre.util.SequenceParserFactory;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class RunMarDRe extends Configured implements Tool {

	// Timers
	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_PREPARE_PSEQ = 2;
	private static final int T_REMOVE_DUP = 3;
	private static final int T_PRINT_SEQ = 4;

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new RunMarDRe(), args));
	}

	private static Job configureSingleEndJob(org.apache.hadoop.conf.Configuration conf, Class<? extends TextInputFormat> inputFormatClass) throws IOException {

		Job job = Job.getInstance(conf, "MarDRe_SingleEnd");
		job.setJarByClass(RunMarDRe.class);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

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

		Job job = Job.getInstance(conf, "MarDRe_PairedEnd");
		job.setJarByClass(RunMarDRe.class);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

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
		Job job = Job.getInstance(conf, "MarDRe_HadoopJoinSequences");
		job.setJarByClass(RunMarDRe.class);

		// Max Mapper attempts
		job.setMaxMapAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (Configuration.COMPRESS_INTERMEDIATE_OUTPUT) {
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
		Job job = Job.getInstance(conf, "MarDRe_JoinSequences");
		job.setJarByClass(RunMarDRe.class);

		// Max Mapper and Reducer attempts
		job.setMaxMapAttempts(1);
		job.setMaxReduceAttempts(1);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

		// I/O Format classes
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Compress options
		if (Configuration.COMPRESS_INTERMEDIATE_OUTPUT) {
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

	private static boolean merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			return false;

		OutputStream out = dstFS.create(dstFile);

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			Arrays.sort(contents);
			for (int i = 0; i < contents.length; i++) {
				if (contents[i].isFile()) {
					InputStream in = srcFS.open(contents[i].getPath());
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


		if (deleteSource) {
			return srcFS.delete(srcDir, true);
		} else {
			return true;
		}
	}

	private static void isInputCompressed(org.apache.hadoop.conf.Configuration conf, Path inputPath) {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);

		if (codec == null)
			return;

		throw new UnsupportedOperationException("Input file ("+inputPath+") is compressed using "+codec.getClass().getSimpleName());
	}

	@Override
	public int run(String[] args) throws Exception {

		Job jobRemoveDuplicate = null;
		Path inputPath1 = null, inputPath2 = null;
		Path tmpInputPath1 = null, tmpInputPath2 = null;
		Path outputPath1 = null, outputPath2 = null;
		Path tmpOutputPath1 = null, tmpOutputPath2 = null;
		long inputPath1Length = 0, inputPath2Length = 0;
		boolean status = true;

		timer.start(T_TOTAL);

		timer.start(T_INIT);

		System.out.println("MarDRe: starting from MARDRE_HOME = "+Configuration.MARDRE_HOME);

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
		RunMarDRe.isInputCompressed(conf, inputPath1);

		if (options.isPaired()) {
			inputPath2 = fs.resolvePath(new Path(options.getReadFileName2()));
			inputPath2Length = fs.getFileStatus(inputPath1).getLen();
			RunMarDRe.isInputCompressed(conf, inputPath2);		
		}

		// Determine the file format
		FileFormat format = options.getInputFileFormat();

		if (format == FileFormat.FILE_FORMAT_UNKNOWN) {
			// Try to autodetect the file format
			System.out.println("MarDRe: detecting input file format from "+inputPath1);
			format = SequenceParserFactory.autoDetectFileFormat(fs, inputPath1, options.getPrefixLength());
		}

		// Set input format class
		Class<? extends TextInputFormat> inputFormatClass = null;

		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			System.out.println("MarDRe: FASTQ format identified");
			inputFormatClass = FastQInputFormat.class;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			System.out.println("MarDRe: FASTA format identified");
			inputFormatClass = FastAInputFormat.class;
		}

		// Load MarDRe configuration
		if (!Configuration.load(conf, options, inputPath1, format))
			return -1;

		// Get output files
		outputPath1 = new Path(options.getWriteFileName1());
		tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);

		if (fs.exists(tmpOutputPath1))
			fs.delete(tmpOutputPath1, true);

		// Print command line options and configuration parameters
		System.out.println("MarDRe: prefix length = "+options.getPrefixLength());
		System.out.println("MarDRe: allowed mismatches = "+options.getMisMatch());
		System.out.println("MarDRe: I/O file buffer size = "+Configuration.IO_FILE_BUFFER_SIZE+" bytes");
		System.out.println("MarDRe: in-Mapper combiner = "+Configuration.IN_MAPPER_COMBINER);
		System.out.println("MarDRe: in-Mapper combiner cache = "+Configuration.IN_MAPPER_COMBINER_CACHE_SIZE+" entries");
		System.out.println("MarDRe: cluster initial capacity = "+Configuration.CLUSTER_LIST_INITIAL_CAPACITY+" entries");

		if (Configuration.COMPRESS_MAP_OUTPUT || Configuration.COMPRESS_INTERMEDIATE_OUTPUT) {
			try {
				SnappyCodec.checkNativeCodeLoaded();
			} catch (Exception e) {
				System.out.println("MarDRe-Warning: "+e.getMessage());
				Configuration.COMPRESS_MAP_OUTPUT = false;
			}
			conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, Configuration.COMPRESS_MAP_OUTPUT);
			conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
		}

		if (Configuration.COMPRESS_MAP_OUTPUT)
			System.out.println("MarDRe: using map output Snappy compression");

		if (options.isPaired()) {
			System.out.println("MarDRe: running in paired-end mode");

			if (Configuration.COMPRESS_INTERMEDIATE_OUTPUT)
				System.out.println("MarDRe: using intermediate output Snappy compression");

			// Paired-end mode writes a second output file
			outputPath2 = new Path(options.getWriteFileName2());
			tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);
			tmpInputPath1 = new Path(Configuration.TEMP_INPUT_PATH_1);
			tmpInputPath2 = new Path(Configuration.TEMP_INPUT_PATH_2);
			System.out.println("MarDRe: input path = "+inputPath1+" ("+inputPath1Length+" bytes)");
			System.out.println("MarDRe: input path = "+inputPath2+" ("+inputPath2Length+" bytes)");

			if (inputPath1Length != inputPath2Length) {
				System.err.println("MarDRe: input files must be the same size in paired-end mode");
				return -1;
			}

			if (fs.exists(tmpInputPath1))
				fs.delete(tmpInputPath1, true);
			if (fs.exists(tmpInputPath2))
				fs.delete(tmpInputPath2, true);
			if (fs.exists(tmpOutputPath2))
				fs.delete(tmpOutputPath2, true);

			// Configure job to remove duplicate reads
			jobRemoveDuplicate = RunMarDRe.configurePairedEndJob(conf);
			FileOutputFormat.setOutputPath(jobRemoveDuplicate, tmpOutputPath1);
			// Add additional output for the second file
			MultipleOutputs.addNamedOutput(jobRemoveDuplicate, "output2", TextOutputFormat.class, NullWritable.class, Sequence.class);

			if (Configuration.PAIRED_JOIN) {
				// Paired-end mode using a map-side Hadooop join needs two map-only jobs to partition both input files
				Job jobPairedSequences1 = RunMarDRe.configureHadoopJoinSequencesJob(conf, inputFormatClass);
				Job jobPairedSequences2 = RunMarDRe.configureHadoopJoinSequencesJob(conf, inputFormatClass);
				FileInputFormat.addInputPath(jobPairedSequences1, inputPath1);
				FileInputFormat.addInputPath(jobPairedSequences2, inputPath2);
				FileOutputFormat.setOutputPath(jobPairedSequences1, tmpInputPath1);
				FileOutputFormat.setOutputPath(jobPairedSequences2, tmpInputPath2);
				// Map-side inner join of both partitioned files
				String joinExpression = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, tmpInputPath1, tmpInputPath2);
				jobRemoveDuplicate.getConfiguration().set("mapreduce.join.expr", joinExpression);
				timer.stop(T_INIT);

				// Run first job
				System.out.println("MarDRe: using map-side join. Partitioning input paths...");
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
				Job jobPairedSequences = RunMarDRe.configureJoinSequencesJob(conf, inputFormatClass);
				FileInputFormat.addInputPath(jobPairedSequences, inputPath1);
				FileInputFormat.addInputPath(jobPairedSequences, inputPath2);
				FileOutputFormat.setOutputPath(jobPairedSequences, tmpInputPath1);
				FileInputFormat.addInputPath(jobRemoveDuplicate, tmpInputPath1);
				timer.stop(T_INIT);

				// Run job
				System.out.println("MarDRe: pairing sequences...");
				timer.start(T_PREPARE_PSEQ);
				status = jobPairedSequences.waitForCompletion(true);
				timer.stop(T_PREPARE_PSEQ);
			}
		} else {
			// Single-end mode reads and writes a single file
			System.out.println("MarDRe: running in single-end mode");
			System.out.println("MarDRe: input path = "+inputPath1+" ("+inputPath1Length+" bytes)");

			// Configure job
			jobRemoveDuplicate = RunMarDRe.configureSingleEndJob(conf, inputFormatClass);
			FileInputFormat.addInputPath(jobRemoveDuplicate, inputPath1);
			FileOutputFormat.setOutputPath(jobRemoveDuplicate, tmpOutputPath1);
			timer.stop(T_INIT);
		}

		if (status) {
			// Run main job 
			System.out.println("MarDRe: removing duplicate reads...");
			timer.start(T_REMOVE_DUP);
			status = jobRemoveDuplicate.waitForCompletion(true);
			timer.stop(T_REMOVE_DUP);
		}

		if (!status) {
			System.out.println("MarDRe: job failed!");
			return -1;
		}

		Path success = new Path(tmpOutputPath1+"/_SUCCESS");

		if (fs.exists(success))
			fs.delete(success, true);

		if (Configuration.MERGE) {
			if (fs.exists(outputPath1))
				fs.delete(outputPath1, true);

			// Merge output files
			System.out.print("MarDRe: merging output files...");

			timer.start(T_PRINT_SEQ);
			RunMarDRe.merge(fs, tmpOutputPath1, fs, outputPath1, Configuration.DELETE_TEMP, conf);

			if (options.isPaired()) {
				if (fs.exists(outputPath2))
					fs.delete(outputPath2, true);

				RunMarDRe.merge(fs, tmpOutputPath2, fs, outputPath2, Configuration.DELETE_TEMP, conf);
			}
			timer.stop(T_PRINT_SEQ);
			System.out.println("done");
		} else {
			outputPath1 = tmpOutputPath1;
			outputPath2 = tmpOutputPath2;
		}

		System.out.println("MarDRe: output path = "+fs.getUri()+outputPath1);

		// Cleanup
		if (options.isPaired()) {
			System.out.println("MarDRe: output path = "+fs.getUri()+outputPath2);
			if (Configuration.DELETE_TEMP) {
				fs.delete(tmpInputPath1, true);
				fs.delete(tmpInputPath2, true);
			}
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
		if (options.isPaired())
			System.out.format(java.util.Locale.ENGLISH,"\tSequences were paired in %.3f seconds\n", timer.readTotal(T_PREPARE_PSEQ));
		System.out.format(java.util.Locale.ENGLISH,"\tDuplicate reads were removed in %.3f seconds\n", timer.readTotal(T_REMOVE_DUP));
		if (Configuration.MERGE)
			System.out.format(java.util.Locale.ENGLISH,"\tOutput files were merged in %.3f seconds\n", timer.readTotal(T_PRINT_SEQ));

		return 0;
	}
}
