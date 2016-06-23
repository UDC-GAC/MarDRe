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

public class FastQRecordReader extends RecordReader<LongWritable, Text> {
	private int linesToProcess;
	private LineReader in;
	private LongWritable key;
	private Text value;
	private Text newLine;
	private Text temp;
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

	private int maxBytesToConsume(long pos) {
		return (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);
		this.linesToProcess = 4;
		this.key = new LongWritable();
		this.value = new Text();
		this.newLine = new Text();
		this.temp = new Text();
		this.start = split.getStart();
		this.pos = this.start;
		this.end = start + split.getLength();
		FSDataInputStream filein = fs.open(file);
		filein.seek(start);
		this.in = new LineReader(filein, conf);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int i = 0;
		long firstRead = 0, secondRead = 0;
		boolean found = false;
		newLine.clear();
		value.clear();
		temp.clear();
	
		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		if (this.pos >= this.end) {
			//System.out.println("nextKeyValue: not in chunk");
			return false;
		}

		key.set(this.pos);

		while(i < linesToProcess) {
			firstRead = in.readLine(newLine, maxLineLength, maxBytesToConsume(this.pos));
			this.pos += firstRead;
			i++;

			//System.out.println("nextKeyValue: line "+i+", read "+firstRead+", start "+start+", pos "+pos+", end "+end);

			if (firstRead == 0) {
				//System.out.println("nextKeyValue: EOF");
				return false;
			}

			//System.out.println("nextKeyValue: value "+newLine.toString());

			if (found == true && i == linesToProcess) {
				//System.out.println("nextKeyValue: last line and starting @ has been previously found");
				value.append(newLine.getBytes(), 0, newLine.getLength());
				value.append(endline.getBytes(), 0, endline.getLength());
				continue;
			}

			if (newLine.charAt(0) == '@') {
				//System.out.println("nextKeyValue: starting @ has been found at line "+i);
				found = true;

				secondRead = in.readLine(temp, maxLineLength, maxBytesToConsume(this.pos));
				value.clear();
				this.pos += secondRead;

				if (secondRead == 0) {
					//System.out.println("nextKeyValue: next line is EOF");
					return false;
				}

				//System.out.println("nextKeyValue: next line (read "+secondRead+") "+temp);

				if (temp.charAt(0) != '@') {
					//System.out.println("nextKeyValue: next line has no starting @ (new record)");
					key.set(this.pos-firstRead);
					i = 2;
					newLine.append(endline.getBytes(), 0, endline.getLength());
					newLine.append(temp.getBytes(), 0, temp.getLength());
				} else {
					//System.out.println("nextKeyValue: next line has starting @ (new record, discard previous data)");
					key.set(this.pos);
					i = 1;
					value.append(temp.getBytes(), 0, temp.getLength());
					value.append(endline.getBytes(), 0, endline.getLength());
					continue;
				}
			}

			if (found == false) {
				if (newLine.find("@", 1) != -1) {
					//System.out.println("nextKeyValue: non-starting @ has been found at line "+i+ "(discard previous data)");
					value.clear();
					key.set(this.pos);
					i = 0;
					continue;
				}

				if (i == linesToProcess) {
					//System.out.println("nextKeyValue: last line and no starting @ has been found (discard previous data)");
					value.clear();
					key.set(this.pos);
					i = 0;
					continue;
				}
			} 

			value.append(newLine.getBytes(), 0, newLine.getLength());
			value.append(endline.getBytes(), 0, endline.getLength());
		}

		//System.out.println("nextKeyValue: start "+start+", pos "+pos+", end "+end);

		return true;
	}
}
