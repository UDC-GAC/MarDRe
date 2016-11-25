package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;

public class JoinSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

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
