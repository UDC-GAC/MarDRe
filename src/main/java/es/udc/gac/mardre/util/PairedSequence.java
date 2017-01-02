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
package es.udc.gac.mardre.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairedSequence implements Writable {

	private static short[] numMissMatch = new short[1];

	private Sequence firstSeq;
	private Sequence secondSeq;

	public PairedSequence() {
		this.firstSeq = null;
		this.secondSeq = null;
	}

	public PairedSequence(Sequence first, Sequence second) {
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

	public long getKey() {
		return this.firstSeq.getKey();
	}

	public boolean equal(PairedSequence seq2, short missMatch) {
		numMissMatch[0] = 0;
		if (this.firstSeq.equal(seq2.firstSeq, missMatch, numMissMatch)) {
			return this.secondSeq.equal(seq2.secondSeq, missMatch, numMissMatch);
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
