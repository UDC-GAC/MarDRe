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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.Sequence;

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

		List<Sequence> sequences = new ArrayList<Sequence>();
		List<Short> dist = new ArrayList<Short>();
		short numMissMatch;
		
		// Get and write the first sequence
		Sequence first = new Sequence(values.iterator().next());
		context.write(nullKey, first);

		for (Sequence newSeq : values) {
			numMissMatch = first.dist(newSeq);

			if (numMissMatch > missMatch) {
				sequences.add(new Sequence(newSeq));
				dist.add(numMissMatch);
			}
		}

		Sequence seq1, seq2;
		int diff, size, j, i;
		size = sequences.size();

		for (i=0; i<size-1; i++) {
			seq1 = sequences.get(i);

			if (!seq1.isDuplicate()) {
				for (j=i+1; j<size; j++) {
					seq2 = sequences.get(j);

					if (!seq2.isDuplicate()) {
						diff = dist.get(i).shortValue() - dist.get(j).shortValue();
						diff = (diff < 0) ? -diff : diff;

						if (diff <= missMatch) {
							if(seq1.equal(seq2, missMatch)) {
								seq2.setDuplicate(true);
							}
						}
					}
				}
			}
		}

		for (Sequence seq : sequences) {
			if (!seq.isDuplicate()) {
				context.write(nullKey, seq);
			}
		}

		sequences = null;
		dist = null;
		first = null;
	}
}
