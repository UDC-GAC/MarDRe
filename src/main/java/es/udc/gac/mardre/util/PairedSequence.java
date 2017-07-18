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
package es.udc.gac.mardre.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairedSequence implements Writable {

	private static short[] numMisMatch = new short[1];

	private Sequence firstSeq;
	private Sequence secondSeq;
	private short dist;

	public PairedSequence() {
		this.firstSeq = null;
		this.secondSeq = null;
		dist = -1;
	}

	public PairedSequence(PairedSequence seq) {
		set(seq);
	}

	public void set(PairedSequence seq) {
		this.firstSeq = seq.firstSeq;
		this.secondSeq = seq.secondSeq;
		dist = seq.dist;
	}

	public Sequence getFirstSeq() {
		return firstSeq;
	}

	public void setFirstSeq(Sequence firstSeq) {
		this.firstSeq = firstSeq;
		this.firstSeq.setIsFirstSeq(true);
	}

	public Sequence getSecondSeq() {
		return secondSeq;
	}

	public void setSecondSeq(Sequence secondSeq) {
		this.secondSeq = secondSeq;
		this.secondSeq.setIsFirstSeq(false);
	}

	public short getDist() {
		return dist;
	}

	public short setDist(PairedSequence seq2) {
		dist = (short) (firstSeq.setDist(seq2.firstSeq) + secondSeq.setDist(seq2.secondSeq));
		return dist;
	}

	public long getKey() {
		return this.firstSeq.getKey();
	}

	public float getAvgQual() {
		return firstSeq.getAvgQual() + secondSeq.getAvgQual();
	}

	public boolean equal(PairedSequence seq2, short misMatch) {
		numMisMatch[0] = 0;
		if (this.firstSeq.equal(seq2.firstSeq, misMatch, numMisMatch)) {
			return this.secondSeq.equal(seq2.secondSeq, misMatch, numMisMatch);
		}
		return false;
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
