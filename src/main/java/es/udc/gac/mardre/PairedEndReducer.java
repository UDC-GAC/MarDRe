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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.PairedSequence;
import es.udc.gac.mardre.util.Sequence;

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

		for (PairedSequence newSeq : values) {
			insert = true;

			for (PairedSequence seq : sequences) {
				if (newSeq.getFirstSeq().equal(seq.getFirstSeq(), missMatch, true)) {
					if (newSeq.getSecondSeq().equal(seq.getSecondSeq(), missMatch, true)) {
						insert = false;
						break;
					}
				}
			}
			if (insert)
				sequences.add(new PairedSequence(newSeq.getFirstSeq(), newSeq.getSecondSeq()));
		}

		for (PairedSequence seq : sequences) {
			context.write(nullKey, seq.getFirstSeq());
			mos.write("output2", nullKey, seq.getSecondSeq(), Configuration.TEMP_OUTPUT_PATH_2+"/part");
		}
	}
}
