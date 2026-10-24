/*
 * Copyright (C) 2017-2026 Universidade da Coruña
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts serialized {@link LongWritable} keys in the same order as {@link LongWritable.Comparator},
 * reading each key with a single 8-byte load instead of assembling it from eight single-byte reads.
 * The sort of the map output compares keys millions of times per task.
 */
public final class LongKeyComparator extends WritableComparator {

	// A serialized LongWritable is its value in big-endian order
	private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

	public LongKeyComparator() {
		super(LongWritable.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return Long.compare((long) LONG.get(b1, s1), (long) LONG.get(b2, s2));
	}
}
