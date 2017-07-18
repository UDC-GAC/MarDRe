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
 * along with MarDRe. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory;
import es.udc.gac.mardre.util.Timer;

public class ParseSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_SETUP = 1;
	private static final int T_MAP = 2;
	private static Sequence seq = new Sequence();
	private static SeqStringParser parser;
	private static boolean isFirstInputPath = true;
	private static long numInputSequences;

	@Override
	public void run(Context context) throws IOException, InterruptedException {

		timer.start(T_TOTAL);

		Configuration.set(context.getConfiguration());
		int taskID = context.getTaskAttemptID().getTaskID().getId();

		timer.start(T_SETUP);
		setup(context);
		timer.stop(T_SETUP);

		try {
			timer.start(T_MAP);
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
			timer.stop(T_MAP);
		} finally {
			cleanup(context);
		}

		timer.stop(T_TOTAL);

		// Report stats
		System.out.format(java.util.Locale.ENGLISH,"Total time for mapper task %d: %.3f seconds (%d reads)\n", taskID, timer.readTotal(T_TOTAL), numInputSequences);
		System.out.format(java.util.Locale.ENGLISH,"\tSetup time: %.3f seconds\n", timer.readTotal(T_SETUP));
		System.out.format(java.util.Locale.ENGLISH,"\tMap time: %.3f seconds\n", timer.readTotal(T_MAP));
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String inputSplitPath = ((FileSplit) context.getInputSplit()).getPath().toString();
		numInputSequences = 0;

		if (!inputSplitPath.equals(Configuration.INPUT_PATH_1))
			isFirstInputPath = false;

		short prefixLength = (isFirstInputPath)? Configuration.PREFIX_LENGTH : 0;
		parser = SequenceParserFactory.createParser(Configuration.FILE_FORMAT, prefixLength, Configuration.NUMBER_BASES_COMPARED, !isFirstInputPath);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String str = Text.decode(value.getBytes(), 0, value.getLength(), false);
		parser.getSeq(str, seq);
		seq.setIsFirstSeq(isFirstInputPath);
		context.write(key, seq);
		numInputSequences++;
	}
}
