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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.LineReader;

public class FastARecordReader extends RecordReader<LongWritable, Text> {

	private LineReader in;
	private LongWritable key;
	private Text value;
	private Text newLine;
	private long start = 0;
	private long end = 0;
	private long pos = 0;

	@Override
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		}
		else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);
		this.key = new LongWritable();
		this.value = new Text();
		this.newLine = new Text();
		this.start = split.getStart();
		this.pos = this.start;
		this.end = start + split.getLength();
		FSDataInputStream fileIn = fs.open(file);
		fileIn.seek(start);
		int bufferSize = Integer.parseInt(conf.get(Configuration.IO_BUFFER_SIZE_PARAM));
		this.in = new LineReader(fileIn, bufferSize);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		long read = 0;
		boolean found = false;
		value.clear();

		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		if (this.pos >= this.end)
			return false;

		while(true) {
			read = in.readLine(newLine);
			this.pos += read;

			//System.out.println("nextKeyValue: read "+read+", start "+start+", pos "+pos+", end "+end);

			if (read == 0) {
				// EOF
				if (found)
					break;
				return false;
			}

			if (newLine.charAt(0) == '>') {
				//System.out.println("nextKeyValue: starting '>' has been found");

				if (found) {
					in.seek(in.getPos()-read);
					this.pos -= read;
					break;
				} else {
					key.set(this.pos-read);
					found = true;
				}
			}

			if (found)
				value.append(newLine.getBytes(), 0, newLine.getLength());
		}

		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		return true;
	}
}
