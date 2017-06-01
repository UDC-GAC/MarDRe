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

import es.udc.gac.mardre.util.LineReader;

public abstract class SequenceRecordReader extends RecordReader<LongWritable, Text> {

	private FSDataInputStream splitInputStream;
	private SplitCompressionInputStream splitCompressedInputStream;
	private Seekable filePos;
	private boolean isCompressedInput;
	private Decompressor decompressor;
	private LineReader lineReader;
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
			return Math.min(1.0f, (getSplitPosition() - start) / (float)(end - start));
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
		splitInputStream = file.getFileSystem(conf).open(file);

		// Check if input file is compressed
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

		if (codec == null) {
			System.out.println("Input split is not part of a compressed file");
			isCompressedInput = false;

			// Seek to the start of the split
			splitInputStream.seek(start);
			// Create line reader and adjust positions
			lineReader = new LineReader(splitInputStream, conf);
			filePos = splitInputStream;
		} else {
			System.out.println("Input split is part of a compressed file using: "+codec.getClass().getSimpleName());
			isCompressedInput = true;

			if (codec instanceof SplittableCompressionCodec) {
				decompressor = CodecPool.getDecompressor(codec);

				// Get compressed input stream
				splitCompressedInputStream = ((SplittableCompressionCodec) codec)
						.createInputStream(splitInputStream, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);

				// Create line reader and adjust positions
				lineReader = new LineReader(splitCompressedInputStream, conf);
				start = splitCompressedInputStream.getAdjustedStart();
				end = splitCompressedInputStream.getAdjustedEnd();
				filePos = splitCompressedInputStream;
			} else {
				throw new UnsupportedOperationException("Input split ("+file.toString()+") is part of a compressed file using a non-splittable codec: "+codec.getClass().getSimpleName());
			}
		}

		pos = start;

		System.out.println("SequenceRecordReader initialized: start "+start+", end "+end+", splitPos "+getSplitPosition());
	}

	@Override
	public synchronized void close() throws IOException {
		try {
			if (lineReader != null) {
				lineReader.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}
		}
	}

	protected int readLine(Text str) throws IOException {
		int bytesRead = lineReader.readLine(str);
		pos += bytesRead;
		return bytesRead;
	}

	protected void seek(long pos) throws IOException {
		lineReader.seek(pos);
	}

	protected long getLineReaderPosition() throws IOException {
		return lineReader.getPos();
	}

	protected long getSplitPosition() throws IOException {
		if (!isCompressedInput) {
			return pos;
		} else {
			return filePos.getPos();
		}
	}
}
