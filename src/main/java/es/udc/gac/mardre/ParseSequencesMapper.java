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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
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

public class ParseSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private static Sequence seq = new Sequence();
	private static SeqStringParser parser;
	private static boolean isFirstInputPath = true;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration.setParameters(context.getConfiguration());
		String inputSplitPath = ((FileSplit) context.getInputSplit()).getPath().toString();

		if (!inputSplitPath.equals(Configuration.inputPath1))
			isFirstInputPath = false;

		if (isFirstInputPath)
			parser = SequenceParserFactory.createParser(Configuration.fileFormat, Configuration.prefixLength);
		else
			parser = SequenceParserFactory.createParser(Configuration.fileFormat, (short) 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (parser.getSeq(value.toString(), seq) != 0) {
			seq.setIsFirstSeq(isFirstInputPath);
			context.write(key, seq);
		}
	}
}
