package es.udc.gac.pardre.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import es.udc.gac.pardre.util.SequenceFactory.FileFormat;

public class PairedSequence implements Writable {

	private Sequence firstSeq;
	private Sequence secondSeq;
	private FileFormat format;

	PairedSequence() {
		this.firstSeq = null;
		this.secondSeq = null;
	}

	PairedSequence(Sequence first, Sequence second) {
		this.firstSeq = first;
		this.secondSeq = second;
	}

	public Sequence getFirstSeq() {
		return firstSeq;
	}

	public void setFirstSeq(Sequence firstSeq) {
		this.firstSeq = firstSeq;
	}

	public Sequence getSecondSeq() {
		return secondSeq;
	}

	public void setSecondSeq(Sequence secondSeq) {
		this.secondSeq = secondSeq;
	}

	public FileFormat getFormat() {
		return format;
	}

	public void setFormat(FileFormat format) {
		this.format = format;
	}

	public long getKey() {
		return this.firstSeq.getKey();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.firstSeq = new Sequence();
		this.secondSeq = new Sequence();
		this.firstSeq.readFields(in);
		this.secondSeq.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.firstSeq.write(out);
		this.secondSeq.write(out);
	}

	@Override
	public String toString() {
		return this.firstSeq.toString()+"\n"+this.secondSeq.toString();
	}
}
