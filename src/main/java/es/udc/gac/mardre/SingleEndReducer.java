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
import es.udc.gac.mardre.util.Timer;

public class SingleEndReducer extends Reducer<LongWritable, Sequence, NullWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final NullWritable nullKey = NullWritable.get();
	private static List<Sequence> sequences = new ArrayList<Sequence>(1000);
	private short missMatch;

	@Override
	public void setup(Context context) {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		missMatch = Short.parseShort(job.get(Configuration.MISSMATCH));
	}

	@Override
	public void reduce(LongWritable key, Iterable<Sequence> values,
			Context context) throws IOException, InterruptedException {

		timer.start(T_TOTAL);

		boolean insert;
		int i = 0;

		for (Sequence newSeq : values) {
			insert = true;
			i++;

			for (Sequence seq : sequences) {
				if (newSeq.equal(seq, missMatch)) {
					insert = false;
					break;
				}
			}
			if (insert)
				sequences.add(new Sequence(newSeq));
		}

		for (Sequence seq : sequences) {
			context.write(nullKey, seq);
		}

		sequences.clear();
		
		timer.stop(T_TOTAL);

		if (i > 2000)
			System.out.format(java.util.Locale.ENGLISH,"Time to analyze %d paired reads: %.2f seconds\n", i, timer.read(T_TOTAL));
	}
}
