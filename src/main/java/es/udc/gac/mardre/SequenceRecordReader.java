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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import es.udc.gac.mardre.util.CompressedLineReader;
import es.udc.gac.mardre.util.Configuration;
import es.udc.gac.mardre.util.LineReader;

public abstract class SequenceRecordReader extends RecordReader<LongWritable, Text> {

	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	protected LineReader in;
	protected LongWritable key;
	protected Text value;
	protected long start;
	protected long end;
	protected long pos;

	public SequenceRecordReader() {
		key = new LongWritable();
		value = new Text();
		start = pos = end = 0;
	}

	public abstract boolean nextKeyValue() throws IOException;

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
			return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

		org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
		FileSplit split = (FileSplit) genericSplit;
		Path file = split.getPath();
		start = split.getStart();
		end = start + split.getLength();

		// open the file
		System.out.println("Open input split "+split.toString());
		fileIn = file.getFileSystem(conf).open(file);

		// Check if input file is compressed
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

		if (codec == null) {
			System.out.println("Input split is not part of a compressed file");
			isCompressedInput = false;

			// Create line reader and seek to the start of the split
			in = new LineReader(fileIn, Configuration.getIOFileBufferSize(conf));
			fileIn.seek(start);
			filePosition = fileIn;
		} else {
			System.out.println("Input split is part of a compressed file using the codec: "+codec.getClass().getSimpleName());
			isCompressedInput = true;

			if (codec instanceof SplittableCompressionCodec) {
				decompressor = CodecPool.getDecompressor(codec);

				// Get compression input stream
				SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
						.createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);

				// Create compressed line reader and adjust positions
				in = new CompressedLineReader(cIn, Configuration.getIOFileBufferSize(conf));
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;
			} else {
				throw new UnsupportedOperationException("Input split ("+file.toString()+") is part of a compressed file using a non-splittable codec: "+codec.getClass().getSimpleName());
			}
		}

		pos = start;
	}

	@Override
	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}
		}
	}

	protected long getFilePosition() throws IOException {
		long ret;

		if (isCompressedInput && filePosition != null) {
			ret = filePosition.getPos();
		} else {
			ret = pos;
		}
		return ret;
	}
}
