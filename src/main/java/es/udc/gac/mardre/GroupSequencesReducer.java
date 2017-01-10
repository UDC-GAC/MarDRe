/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MaRDRe.
 * 
 * MaRDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MaRDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MaRDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;

public class GroupSequencesReducer extends Reducer<LongWritable, Sequence, LongWritable, PairedSequence> {

	private static PairedSequence pairedSeq = new PairedSequence();
	private static LongWritable longKey = new LongWritable();

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
