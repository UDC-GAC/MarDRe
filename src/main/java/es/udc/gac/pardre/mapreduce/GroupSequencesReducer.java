package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.pardre.util.PairedSequence;
import es.udc.gac.pardre.util.Sequence;

public class GroupSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

	private PairedSequence pairedSeq = new PairedSequence();
	private LongWritable longKey = new LongWritable();

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		int i = 0;

		for (Sequence seq : values) {
			Sequence newSeq = new Sequence(seq);
			i++;

			if (seq.isFirstSeq()) {
				pairedSeq.setFirstSeq(newSeq);
			} else {
				pairedSeq.setSecondSeq(newSeq);
			}
		}

		if (i != 2)
			throw new IndexOutOfBoundsException("PairedSequence consists of "+i+" sequence/s");

		longKey.set(pairedSeq.getKey());
		context.write(longKey, pairedSeq);
	}
}
