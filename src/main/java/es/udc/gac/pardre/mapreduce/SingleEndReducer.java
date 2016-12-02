package es.udc.gac.pardre.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.Sequence;

public class SingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

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

			for (Sequence seq : sequences) {
				if (new_seq.equal(seq, missMatch, true)) {
					insert = false;
					break;
				}
			}
			if (insert)
				sequences.add(new Sequence(new_seq));
		}

		for (Sequence seq : sequences) {
			context.write(nullKey, seq);
		}
	}
}
