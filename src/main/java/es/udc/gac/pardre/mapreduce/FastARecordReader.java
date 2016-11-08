package es.udc.gac.pardre.mapreduce;

import java.io.IOException;

import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FastARecordReader extends RecordReader<LongWritable, Text> {

	private LineReader in;
	private LongWritable key;
	private Text value;
	private Text newLine;
	private long start = 0;
	private long end = 0;
	private long pos = 0;
	private int maxLineLength = Integer.MAX_VALUE;
	private final Text endline = new Text("\n");

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
		Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);
		this.key = new LongWritable();
		this.value = new Text();
		this.newLine = new Text();
		this.start = split.getStart();
		this.pos = this.start;
		this.end = start + split.getLength();
		FSDataInputStream filein = fs.open(file);
		filein.seek(start);
		this.in = new LineReader(filein, conf);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		long read = 0;
		boolean found = false;
		newLine.clear();
		value.clear();

		System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		if (this.pos >= this.end) {
			//System.out.println("nextKeyValue: not in chunk");
			return false;
		}

		key.set(this.pos);

		while(true) {
			read = in.readLine(newLine, maxLineLength);
			this.pos += read;
	
			System.out.println("nextKeyValue: read "+read+", start "+start+", pos "+pos+", end "+end);

			if (read == 0) {
				System.out.println("nextKeyValue: EOF");
				return false;
			}

			System.out.println("nextKeyValue: value <'"+newLine.toString()+"'>");

			if (newLine.getLength() == 0) {
				System.out.println("nextKeyValue: newline char has been read (ignore)");
				continue;
			}

			if (newLine.charAt(0) == '>') {
				System.out.println("nextKeyValue: starting '>' has been found");

				if (found == true) {
					System.out.println("nextKeyValue: new record detected, return previous data");
					break;
				} else {
					found = true;
				}

				value.append(newLine.getBytes(), 0, newLine.getLength());
				value.append(endline.getBytes(), 0, endline.getLength());
			}
		}

		System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		return true;
	}
}