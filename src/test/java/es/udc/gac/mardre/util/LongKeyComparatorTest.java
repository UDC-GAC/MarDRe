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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.junit.Test;

public class LongKeyComparatorTest {

	private static final RawComparator<LongWritable> REFERENCE = new LongWritable.Comparator();
	private static final RawComparator<LongWritable> comparator = new LongKeyComparator();

	// Serialized key preceded by 'offset' bytes of padding, as in the sort buffers
	private static byte[] serialize(long value, int offset) throws Exception {
		DataOutputBuffer out = new DataOutputBuffer();

		for (int i = 0; i < offset; i++)
			out.writeByte(0x55);

		new LongWritable(value).write(out);
		return java.util.Arrays.copyOf(out.getData(), out.getLength());
	}

	private static void assertSameOrder(long a, long b, int offsetA, int offsetB) throws Exception {
		byte[] x = serialize(a, offsetA);
		byte[] y = serialize(b, offsetB);
		int expected = Integer.signum(REFERENCE.compare(x, offsetA, 8, y, offsetB, 8));
		int actual = Integer.signum(comparator.compare(x, offsetA, 8, y, offsetB, 8));
		assertEquals("keys " + a + " and " + b, expected, actual);
	}

	@Test
	public void sortsTheBoundaryValuesLikeLongWritable() throws Exception {
		long[] values = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -4294967296L, -4294967295L, -2, -1, 0, 1, 2,
				0xFFFFFFFFL, 0x100000000L, 0x7FFFFFFFL, 0x80000000L, Long.MAX_VALUE - 1, Long.MAX_VALUE };

		for (long a : values)
			for (long b : values)
				assertSameOrder(a, b, 0, 0);
	}

	@Test
	public void sortsRandomKeysLikeLongWritableWithAnyOffset() throws Exception {
		Random r = new Random(7);

		for (int i = 0; i < 20000; i++) {
			long a = r.nextLong();
			// Often share the high part, as keys of reads with a common prefix do
			long b = (r.nextInt(3) == 0) ? a ^ (1L << r.nextInt(64)) : r.nextLong();
			assertSameOrder(a, b, r.nextInt(9), r.nextInt(9));
		}
	}

	@Test
	public void comparesEqualKeysAsEqual() throws Exception {
		assertEquals(0, comparator.compare(serialize(42L, 3), 3, 8, serialize(42L, 0), 0, 8));
	}

	@Test
	public void comparesWritableObjectsLikeLongWritable() {
		LongKeyComparator c = new LongKeyComparator();
		assertEquals(-1, Integer.signum(c.compare(new LongWritable(-5), new LongWritable(3))));
		assertEquals(1, Integer.signum(c.compare(new LongWritable(3), new LongWritable(-5))));
		assertEquals(0, c.compare(new LongWritable(3), new LongWritable(3)));
	}
}
