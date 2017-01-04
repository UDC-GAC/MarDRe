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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public class ParseSequencesMapper extends Mapper<LongWritable, Text, LongWritable, Sequence> {

	private Sequence seq = new Sequence();
	private SeqStringParser parser;
	private short prefixLength;
	private char minQual;
	private boolean isFirstInputPath = true;
	private FileFormat format;

	@Override
	public void setup(Context context) throws IOException {
		org.apache.hadoop.conf.Configuration job = context.getConfiguration();
		format = FileFormat.values()[Integer.parseInt(job.get(Configuration.FILE_FORMAT))];
		prefixLength = Short.parseShort(job.get(Configuration.PREFIXLENGTH));
		minQual = (char) Integer.parseInt(job.get(Configuration.MINQUAL));
		parser = SequenceParserFactory.createParser(format, prefixLength, minQual);
		String inputPath1 = job.get(Configuration.INPUT_PATH_1);
		String inputSplitPath = ((FileSplit) context.getInputSplit()).getPath().toString();

		if (!inputSplitPath.equals(inputPath1))
			isFirstInputPath = false;
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
