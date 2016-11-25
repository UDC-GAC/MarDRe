package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;

public class PairedEndReducer extends Reducer<LongWritable, PairedSequence, NullWritable, Sequence> {

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
