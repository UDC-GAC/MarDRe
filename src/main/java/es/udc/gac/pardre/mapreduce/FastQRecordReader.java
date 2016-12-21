package es.udc.gac.pardre.mapreduce;

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

import es.udc.gac.pardre.util.Configuration;
import es.udc.gac.pardre.util.LineReader;

public class FastQRecordReader extends RecordReader<LongWritable, Text> {

	private static final int linesToProcess = 4;
	
	private LineReader in;
	private LongWritable key;
	private Text value;
	private Text newLine;
	private Text temp;
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
		this.temp = new Text();
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
		int i = 0;
		long firstRead = 0, secondRead = 0;
		boolean found = false;
		value.clear();

		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		if (this.pos >= this.end)
			return false;

		while(i < linesToProcess) {
			firstRead = in.readLine(newLine);
			this.pos += firstRead;
			i++;

			if (firstRead == 0) //EOF
				return false;

			if (found && i == linesToProcess) {
				//System.out.println("nextKeyValue: last line and starting '@' has been previously found");
				value.append(newLine.getBytes(), 0, newLine.getLength());
				break;
			}

			if (newLine.charAt(0) == '@') {
				//System.out.println("nextKeyValue: starting '@' has been found at line "+i);
				found = true;
				key.set(this.pos-firstRead);

				secondRead = in.readLine(temp);
				this.pos += secondRead;

				if (secondRead == 0) //EOF
					return false;

				if (temp.charAt(0) != '@') {
					i = 2;
					newLine.append(temp.getBytes(), 0, temp.getLength());
				} else {
					i = 1;
					key.set(this.pos-secondRead);
					value.append(temp.getBytes(), 0, temp.getLength());
					continue;
				}
			}

			if (found) {
				value.append(newLine.getBytes(), 0, newLine.getLength());
			} else if (i == linesToProcess) {
				//System.out.println("nextKeyValue: last line and no starting '@' has been found (discard previous data)");
				i = 0;
			}
		}

		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		return true;
	}
}
