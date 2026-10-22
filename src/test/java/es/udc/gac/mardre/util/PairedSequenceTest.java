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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PairedSequenceTest {

	private static final short PREFIX_LENGTH = 5;
	private static final SeqStringParser PARSER = new FastQSeqStringParser(PREFIX_LENGTH, (short) -1);
	private static final String BASES = "ACGTAAAAAAAAAAAAAAAAAAAA";

	private static Sequence parse(String bases) {
		Sequence seq = new Sequence();
		PARSER.getSeq("@read\n" + bases + "\n+\n" + "I".repeat(bases.length()), seq, true);
		return seq;
	}

	/** The bases of BASES with the given positions (of the suffix) changed to C. */
	private static String mutate(int... positions) {
		char[] bases = BASES.toCharArray();
		for (int pos : positions)
			bases[pos] = 'C';
		return new String(bases);
	}

	private static PairedSequence pair(String bases1, String bases2) {
		PairedSequence pair = new PairedSequence();
		pair.setFirstSeq(parse(bases1));
		pair.setSecondSeq(parse(bases2));
		return pair;
	}

	@Test
	public void identicalPairsAreEqual() {
		assertTrue(pair(BASES, BASES).equal(pair(BASES, BASES), (short) 0));
	}

	@Test
	public void aMismatchInEitherReadMakesPairsDifferentWithoutTolerance() {
		assertFalse(pair(BASES, BASES).equal(pair(mutate(6), BASES), (short) 0));
		assertFalse(pair(BASES, BASES).equal(pair(BASES, mutate(6)), (short) 0));
	}

	@Test
	public void mismatchesOfBothReadsAreAccumulated() {
		PairedSequence reference = pair(BASES, BASES);

		assertTrue(reference.equal(pair(mutate(6), mutate(7)), (short) 2));
		assertTrue(reference.equal(pair(mutate(6, 7), BASES), (short) 2));
		assertTrue(reference.equal(pair(BASES, mutate(6, 7)), (short) 2));
		assertFalse(reference.equal(pair(mutate(6, 7), mutate(8)), (short) 2));
		assertFalse(reference.equal(pair(mutate(6), mutate(7, 8)), (short) 2));
		assertFalse(reference.equal(pair(mutate(6, 7, 8), BASES), (short) 2));
	}

	@Test
	public void theResultDoesNotDependOnPreviousComparisons() {
		PairedSequence reference = pair(BASES, BASES);
		PairedSequence near = pair(mutate(6), mutate(7));

		// A failed comparison must not leave mismatches counted for the next one
		assertFalse(reference.equal(pair(mutate(6, 7, 8), mutate(9, 10)), (short) 2));
		assertTrue(reference.equal(near, (short) 2));
		assertFalse(reference.equal(near, (short) 1));
		assertTrue(reference.equal(near, (short) 2));
	}

	@Test
	public void theDistanceIsTheSumOfTheDistancesOfBothReads() {
		PairedSequence reference = pair(BASES, BASES);
		PairedSequence other = pair(mutate(6, 7), mutate(8));

		assertEquals(3, other.setDist(reference));
	}

	private static PairedSequence pair(String name, String quals1, String quals2) {
		PairedSequence pair = new PairedSequence();
		pair.setFirstSeq(parse(name, quals1));
		pair.setSecondSeq(parse(name, quals2));
		return pair;
	}

	private static Sequence parse(String name, String quals) {
		Sequence seq = new Sequence();
		PARSER.getSeq(name + "\n" + BASES + "\n+\n" + quals, seq, true);
		return seq;
	}

	private static String quals(char c) {
		return String.valueOf(c).repeat(BASES.length());
	}

	@Test
	public void aPairIsBetterWhenTheQualityOfBothReadsIsHigher() {
		PairedSequence high = pair("@z", quals('5'), quals('I'));
		PairedSequence low = pair("@a", quals('5'), quals('?'));

		assertTrue(high.isBetterThan(low));
		assertFalse(low.isBetterThan(high));
	}

	@Test
	public void withTheSameQualityTheLowestNameOfTheFirstReadIsBetter() {
		PairedSequence a = pair("@a", quals('5'), quals('I'));
		// Same summed quality with the scores swapped between the reads
		PairedSequence b = pair("@b", quals('I'), quals('5'));

		assertTrue(a.isBetterThan(b));
		assertFalse(b.isBetterThan(a));
		assertFalse(a.isBetterThan(a));
	}
}
