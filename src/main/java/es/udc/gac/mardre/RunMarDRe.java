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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
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

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

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
		Job job = Job.getInstance(conf, "MarDRe_JoinSequences");
		job.setJarByClass(RunMarDRe.class);

		// Explicitly turn off speculative execution
		job.setSpeculativeExecution(false);

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

	@Override
	public int run(String[] args) throws Exception {

		boolean status = true;

		timer.start(T_TOTAL);

		System.out.println("MarDRe: starting from MARDRE_HOME = "+Configuration.MARDRE_HOME);

		timer.start(T_INIT);
		Configuration.loadConfiguration();
		org.apache.hadoop.conf.Configuration conf = this.getConf();

		// Parse command line options
		Options options = new Options();
		if (!options.parse(args)) {
			options.printUsage(RunMarDRe.class.getName());
			return -1;
		}

		// FOA, detect the file format
		FileSystem fs = FileSystem.get(conf);
		Path inputPath1 = fs.resolvePath(new Path(options.getReadFileName1()));
		System.out.println("MarDRe: detecting file format from input file "+inputPath1);
		FileFormat format = SequenceParserFactory.detectFileFormat(fs, inputPath1, options.getPrefixLength());

		// Set input format class
		Class<? extends TextInputFormat> inputFormatClass = null;

		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			inputFormatClass = FastQInputFormat.class;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			inputFormatClass = FastAInputFormat.class;
		} else {
			return -1;			
		}

		Path outputPath1 = new Path(options.getWriteFileName1());
		Path tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);
		Path outputPath2 = null;
		Path tmpInputPath1 = null;
		Path tmpInputPath2 = null;
		Path tmpOutputPath2 = null;
		Job jobRemoveDuplicate = null;

		if (fs.exists(tmpOutputPath1))
			fs.delete(tmpOutputPath1, true);

		// Set parameters to be used for mapper/reducer classes
		conf.setInt(Configuration.PREFIXLENGTH_PARAM, options.getPrefixLength());
		conf.setInt(Configuration.MISMATCH_PARAM, options.getMisMatch());
		conf.setInt(Configuration.FILE_FORMAT_PARAM, format.ordinal());
		conf.set(Configuration.INPUT_PATH_1_PARAM, inputPath1.toString());
		conf.setInt(Configuration.IO_BUFFER_SIZE_PARAM, Configuration.IO_FILE_BUFFER_SIZE);
		conf.setBoolean(Configuration.IN_MAPPER_COMBINER_PARAM, Configuration.IN_MAPPER_COMBINER);
		conf.setInt(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE_PARAM, Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);

		System.out.println("MarDRe: prefix length = "+options.getPrefixLength());
		System.out.println("MarDRe: allowed mismatches = "+options.getMisMatch());
		System.out.println("MarDRe: input buffer size = "+Configuration.IO_FILE_BUFFER_SIZE);
		System.out.println("MarDRe: in-Mapper combiner = "+Configuration.IN_MAPPER_COMBINER);
		System.out.println("MarDRe: in-Mapper combiner cache size = "+Configuration.IN_MAPPER_COMBINER_CACHE_SIZE);

		if (options.isPaired()) {
			System.out.println("MarDRe: using paired-end mode");

			if (Configuration.COMPRESS_TEMP)
				System.out.println("MarDRe: using Snappy compression");

			// Paired-end mode reads (and writes) a second file
			Path inputPath2 = fs.resolvePath(new Path(options.getReadFileName2()));
			outputPath2 = new Path(options.getWriteFileName2());
			tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);
			tmpInputPath1 = new Path(Configuration.TEMP_INPUT_PATH_1);
			tmpInputPath2 = new Path(Configuration.TEMP_INPUT_PATH_2);
			System.out.println("MarDRe: input path = "+inputPath1);
			System.out.println("MarDRe: input path = "+inputPath2);

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
			// Single-end mode reads (and writes) one file
			System.out.println("MarDRe: using single-end mode");
			System.out.println("MarDRe: input path = "+inputPath1);

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

		if (Configuration.MERGE) {
			if (fs.exists(outputPath1))
				fs.delete(outputPath1, true);

			// Merge output files
			System.out.print("MarDRe: merging output files...");
			timer.start(T_PRINT_SEQ);
			FileUtil.copyMerge(fs, tmpOutputPath1, fs, outputPath1, Configuration.DELETE_TEMP, conf, null);

			if (options.isPaired()) {
				if (fs.exists(outputPath2))
					fs.delete(outputPath2, true);

				FileUtil.copyMerge(fs, tmpOutputPath2, fs, outputPath2, Configuration.DELETE_TEMP, conf, null);
			}
			timer.stop(T_PRINT_SEQ);
			System.out.println("done");
		} else {
			outputPath1 = tmpOutputPath1;
			outputPath2 = tmpOutputPath2;
		}

		System.out.println("MarDRe: job completed successfully!");
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

		System.out.format(java.util.Locale.ENGLISH,"MarDRe: non duplicate reads %d/%d (%.2f %%)\n", outputSequences, inputSequences, percent);
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
