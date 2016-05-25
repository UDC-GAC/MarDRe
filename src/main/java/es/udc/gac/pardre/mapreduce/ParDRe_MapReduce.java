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

public class ParDRe_MapReduce
{

	private static final String PREFIXLENGTHPARAM = "pardre_prefixLength";
	private static final String MINQUALPARAM = "pardre_minQual";
	private static final String MISSMATCHPARAM = "pardre_missMatch";

	public static class ParDReMapper
			extends Mapper<Object, Text, LongWritable, Sequence>
	{
		private Sequence seq = new Sequence();
		private LongWritable longKey = new LongWritable();
		private SeqStringParser parser;
		private short prefixLength;
		private char minQual;

		@Override
		public void setup(Context context)
		{
			Configuration job = context.getConfiguration();
			prefixLength = Short.parseShort(job.get(PREFIXLENGTHPARAM));
			minQual = (char) Integer.parseInt(job.get(MINQUALPARAM));
			parser = new SeqStringParser(prefixLength, minQual);

			System.out.println("ParDreMapper : prefixLength = " + prefixLength
					+ " ; minQual = " + (int) minQual);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException
		{
			if (parser.getSeq(value.toString(), seq) != 0)
			{
				longKey.set(seq.get_key());
				// System.out.println("WRITING " + key + ":" +
				// seq.toStringFull());
				context.write(longKey, seq);
			}
		}
	}

	public static class ParDReReducer
			extends Reducer<LongWritable, Sequence, NullWritable, Sequence>
	{
		private short missMatch;
		private NullWritable caca = NullWritable.get();

		@Override
		public void setup(Context context)
		{
			Configuration job = context.getConfiguration();
			missMatch = Short.parseShort(job.get(MISSMATCHPARAM));

			System.out.println("ParDreReducer : missMatch = " + missMatch);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Sequence> values,
				Context context) throws IOException, InterruptedException
		{
			List<Sequence> sequences = new ArrayList<Sequence>();
			for (Sequence seq : values)
			{
				sequences.add(new Sequence(seq));
			}

			// System.out.println("KEY=" + key + " - " + sequences.size());
			// for (Sequence seq : sequences)
			// {
			// System.out.println(key + ":" + seq);
			// }

			for (int i = 0; i < sequences.size(); i++)
			{
				Sequence seqi = sequences.get(i);
				for (int j = i + 1; j < sequences.size(); j++)
				{
					if (seqi.equal(sequences.get(j), missMatch, true))
					{
						// Delete the second one.
						// In the future we must delete the one with the lowest
						// priority
						System.out.println(seqi.toStringFull() + " equals to <"
								+ sequences.get(j).toStringFull());
						System.out.println(
								"REMOVING " + key + ":" + sequences.get(j));
						sequences.remove(j);
						j--;
					}
				}
				// System.out.println("WRITING " + key + ":" + seqi);
				context.write(caca, seqi);
			}

		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		System.out.println("In ParDRe_MapReduce");

		long startTime = System.currentTimeMillis();
		long time;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Options options = new Options();
		if (!options.parse(otherArgs))
		{
			options.printUsage("ParDRe_MapReduce");
			return;
		}

		conf.setInt(PREFIXLENGTHPARAM, options.get_prefixLength());
		conf.setInt(MINQUALPARAM, options.get_minQual());
		conf.setInt(MISSMATCHPARAM, options.get_missMatch());
		conf.set("textinputformat.record.delimiter", "@");

		Path inputPath = new Path(options.get_readFileName());
		Path tmpPath = new Path(options.get_writeFileName() + ".tmp");
		Path outputPath = new Path(options.get_writeFileName());

		Job job1 = Job.getInstance(conf, "ParDRe_MapReduce_RemoveDuplicate");
		job1.setJarByClass(ParDRe_MapReduce.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Sequence.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Sequence.class);
		job1.setMapperClass(ParDReMapper.class);
		job1.setReducerClass(ParDReReducer.class);

		for (int i = 0; i < otherArgs.length - 1; ++i)
		{
			FileInputFormat.addInputPath(job1, inputPath);
		}
		FileOutputFormat.setOutputPath(job1, tmpPath);

		boolean result = job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "ParDRe_MapReduce_SaveOutput");
		job2.setJarByClass(ParDRe_MapReduce.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Sequence.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Sequence.class);
		job2.setMapperClass(Mapper.class);
		job2.setReducerClass(Reducer.class);
		job2.setNumReduceTasks(1);

		for (int i = 0; i < otherArgs.length - 1; ++i)
		{
			FileInputFormat.addInputPath(job2, tmpPath);
		}
		FileOutputFormat.setOutputPath(job2, outputPath);

		result = job2.waitForCompletion(true);

		// SeqFileParser parser = new SeqFileParser(options);
		// ArrayList<Sequence> sequences = new ArrayList<Sequence>();
		//
		// partialTime = System.currentTimeMillis();
		//
		// // The method getSeq returns 0 if there are no more sequences in the
		// file
		// while(true){
		// Sequence seq = new Sequence();
		// if(parser.getSeq(seq) == 0){
		// break;
		// }
		//
		// sequences.add(seq);
		// }
		//
		// time = System.currentTimeMillis()-partialTime;
		// System.out.println("Read " + sequences.size() + " sequences in " +
		// ((float) time)/1000 + " seconds");
		// partialTime = System.currentTimeMillis();
		//
		// for(int i=0; i<sequences.size()-1; i++){
		// for(int j=i+1; j<sequences.size(); j++){
		// // In this case I indicate with true that we should also look at the
		// prefix.
		// // In general comparisons within the clusters we have to indicate
		// false.
		// if(sequences.get(i).equal(sequences.get(j), options.get_missMatch(),
		// true)){
		// // Delete the second one.
		// // In the future we must delete the one with the lowest priority
		// sequences.remove(j);
		// j--;
		// }
		// }
		// }
		//
		// time = System.currentTimeMillis()-partialTime;
		// System.out.println("Remove duplicate in " + ((float) time)/1000 + "
		// seconds");
		//
		// for(int i=0; i<sequences.size(); i++){
		// parser.printSeq(sequences.get(i));
		// }
		//
		// time = System.currentTimeMillis()-partialTime;
		// System.out.println("Print " + sequences.size() + " sequences in " +
		// ((float) time)/1000 + " seconds");
		//

		tmpPath.getFileSystem(conf).delete(tmpPath, true);

		if (result)
		{
			System.out.println("Application completed successfully");
			time = System.currentTimeMillis() - startTime;
			//
			System.out.println("Done in " + ((float) time) / 1000 + " seconds");
			System.exit(0);
		}
		System.out.println("Application failed to complete successfully");
		System.exit(2);

	}
}
