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

public class OptimizedStringTokenizerTest {

	private static void assertTokens(String str, String... expected) {
		OptimizedStringTokenizer st = new OptimizedStringTokenizer(str, '\n');

		for (String token : expected) {
			assertTrue("missing token '" + token + "' in '" + str + "'", st.hasMoreTokens());
			assertEquals(token, st.nextToken());
		}

		assertFalse("extra tokens in '" + str + "'", st.hasMoreTokens());
	}

	@Test
	public void splitsTheLinesOfAFastqRecord() {
		assertTokens("@r1\nACGT\n+\nIIII", "@r1", "ACGT", "+", "IIII");
	}

	@Test
	public void aTrailingDelimiterDoesNotAddAnEmptyToken() {
		assertTokens("@r1\nACGT\n", "@r1", "ACGT");
	}

	@Test
	public void consecutiveDelimitersGiveEmptyTokens() {
		assertTokens("a\n\nb", "a", "", "b");
		assertTokens("\n\n", "", "");
	}

	@Test
	public void aStringWithoutDelimitersIsOneToken() {
		assertTokens("ACGT", "ACGT");
	}

	@Test
	public void anEmptyStringHasNoTokens() {
		assertTokens("");
	}

	@Test
	public void anotherDelimiterCanBeUsed() {
		OptimizedStringTokenizer st = new OptimizedStringTokenizer("a:b", ':');
		assertEquals("a", st.nextToken());
		assertEquals("b", st.nextToken());
		assertFalse(st.hasMoreTokens());
	}
}
