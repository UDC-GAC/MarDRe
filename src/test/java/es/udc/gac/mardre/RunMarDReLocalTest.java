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
package es.udc.gac.mardre;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Runs the whole tool (RunMarDRe) on Hadoop's local job runner with several map and reduce slots, so
 * the tasks run concurrently inside the JVM, and compares the output with an independent computation
 * of the reads that must be kept.
 */
public class RunMarDReLocalTest {

	private static final int SLOTS = 4;
	private static final int PREFIX_LENGTH = 8;
	private static final int READ_LENGTH = 40;
	private static final int SPLIT_SIZE = 8 * 1024;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private File home;
	/** Extra Hadoop properties of the job, set by the tests that need them. */
	private final Map<String, String> jobProperties = new HashMap<String, String>();

	@Before
	public void setUp() {
		String marDReHome = System.getenv("MARDRE_HOME");
		// The client reads mardre.conf from there: the pom sets it for the tests
		assumeNotNull(marDReHome);
		home = new File(marDReHome);
	}

	/** A read of the input: name, bases and a quality string. */
	private static final class Read {
		final String name;
		final String bases;
		final String quals;

		Read(String name, String bases, String quals) {
			this.name = name;
			this.bases = bases;
			this.quals = quals;
		}

		int qualSum() {
			int sum = 0;
			for (int i = 0; i < quals.length(); i++)
				sum += quals.charAt(i);
			return sum;
		}

		String fastq() {
			return name + "\n" + bases + "\n+\n" + quals + "\n";
		}
	}

	private static String randomBases(Random random, String prefix, int length) {
		StringBuilder sb = new StringBuilder(prefix);
		while (sb.length() < length)
			sb.append("ACGT".charAt(random.nextInt(4)));
		return sb.toString();
	}

	/** Mostly constant scores from a few levels, so the average quality of different reads ties often. */
	private static String randomQuals(Random random, int length) {
		char[] quals = new char[length];
		char level = "5?I".charAt(random.nextInt(3));
		Arrays.fill(quals, level);
		if (random.nextInt(4) == 0)
			quals[random.nextInt(length)] = (char) ('!' + random.nextInt(40));
		return new String(quals);
	}

	private void writeConf(boolean mapJoin, boolean combiner) throws IOException {
		File etc = new File(home, "etc");
		assertTrue(etc.isDirectory() || etc.mkdirs());
		// Every parameter: Configuration keeps the properties of the previous loads
		String conf = "HDFS_BASE_PATH=" + new File(tmp.getRoot(), "base").getAbsolutePath() + "\n"
				+ "HDFS_BLOCK_REPLICATION=1\n"
				+ "MERGE_OUTPUT=false\n"
				+ "DELETE_TEMP=true\n"
				+ "PAIRED_END_MAP_JOIN=" + mapJoin + "\n"
				+ "COMPRESS_MAP_OUTPUT=false\n"
				+ "COMPRESS_INTERMEDIATE_OUTPUT=false\n"
				+ "INPUT_BUFFER_SIZE=65536\n"
				+ "IN_MAPPER_COMBINER=" + combiner + "\n"
				+ "IN_MAPPER_COMBINER_CACHE_SIZE=32\n"
				+ "CLUSTER_LIST_INITIAL_CAPACITY=16\n";
		Files.write(new File(etc, "mardre.conf").toPath(), conf.getBytes(StandardCharsets.UTF_8));
		assertTrue(new File(tmp.getRoot(), "base").isDirectory() || new File(tmp.getRoot(), "base").mkdirs());
	}

	private File write(String name, List<Read> reads) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (Read read : reads)
			sb.append(read.fastq());
		File file = new File(tmp.getRoot(), name);
		Files.write(file.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
		return file;
	}

	/** The lines of a file, decompressed if its extension is the one of a codec (the output of compressed input is compressed). */
	private static List<String> readLines(File file) throws IOException {
		CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodec(new Path(file.getPath()));
		if (codec == null)
			return Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

		InputStream in = codec.createInputStream(Files.newInputStream(file.toPath()));
		try {
			return new java.io.BufferedReader(new java.io.InputStreamReader(in, StandardCharsets.UTF_8)).lines().collect(java.util.stream.Collectors.toList());
		} finally {
			in.close();
		}
	}

	private File writeBzip2(String name, List<Read> reads) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (Read read : reads)
			sb.append(read.fastq());
		File file = new File(tmp.getRoot(), name);
		BZip2Codec codec = ReflectionUtils.newInstance(BZip2Codec.class, new Configuration());
		OutputStream out = codec.createOutputStream(Files.newOutputStream(file.toPath()));
		try {
			out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
		} finally {
			out.close();
		}
		return file;
	}

	private int run(String... args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hadoop.tmp.dir", new File(tmp.getRoot(), "hadoop").getAbsolutePath());
		conf.set("mapreduce.framework.name", "local");
		// Defined by the default configuration of HDFS, which is not loaded here
		conf.setLong("dfs.blocksize", 128 * 1024 * 1024);
		conf.setInt("mapreduce.local.map.tasks.maximum", SLOTS);
		conf.setInt("mapreduce.local.reduce.tasks.maximum", SLOTS);
		// Small splits: several map tasks even for a small input
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", SPLIT_SIZE);
		for (Map.Entry<String, String> property : jobProperties.entrySet())
			conf.set(property.getKey(), property.getValue());
		return ToolRunner.run(conf, new RunMarDRe(), args);
	}

	/** The records (by name) of the part files of an output directory. */
	private Map<String, String> output(String dir) throws IOException {
		Map<String, String> records = new TreeMap<String, String>();
		File[] parts = new File(tmp.getRoot(), "base/MarDRe/tmp/" + dir).listFiles();
		assertTrue("no output in " + dir, parts != null);

		for (File part : parts) {
			if (!part.getName().startsWith("part-"))
				continue;
			List<String> lines = readLines(part);
			assertEquals(0, lines.size() % 4);
			for (int i = 0; i < lines.size(); i += 4) {
				String record = lines.get(i) + "\n" + lines.get(i + 1) + "\n" + lines.get(i + 2) + "\n" + lines.get(i + 3) + "\n";
				assertEquals("duplicated name " + lines.get(i), null, records.put(lines.get(i), record));
			}
		}
		return records;
	}

	private File writeFasta(String name, List<Read> reads) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (Read read : reads)
			sb.append('>').append(read.name.substring(1)).append('\n').append(read.bases).append('\n');
		File file = new File(tmp.getRoot(), name);
		Files.write(file.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
		return file;
	}

	/** The records (by name) of the part files of an output directory of FASTA reads: a name line and a line of bases. */
	private Map<String, String> fastaOutput(String dir) throws IOException {
		Map<String, String> records = new TreeMap<String, String>();
		File[] parts = new File(tmp.getRoot(), "base/MarDRe/tmp/" + dir).listFiles();
		assertTrue("no output in " + dir, parts != null);

		for (File part : parts) {
			if (!part.getName().startsWith("part-"))
				continue;
			List<String> lines = Files.readAllLines(part.toPath(), StandardCharsets.UTF_8);
			assertEquals(0, lines.size() % 2);
			for (int i = 0; i < lines.size(); i += 2)
				assertEquals("duplicated name " + lines.get(i), null, records.put(lines.get(i), lines.get(i) + "\n" + lines.get(i + 1) + "\n"));
		}
		return records;
	}

	/** Without quality scores every duplicate ties: the read with the lowest name is kept. */
	private static Map<String, String> lowestNameOfEachSequence(Iterable<Read> reads) {
		Map<String, Read> lowest = new HashMap<String, Read>();
		for (Read read : reads) {
			Read kept = lowest.get(read.bases);
			if (kept == null || read.name.compareTo(kept.name) < 0)
				lowest.put(read.bases, read);
		}

		Map<String, String> records = new TreeMap<String, String>();
		for (Read read : lowest.values())
			records.put(">" + read.name.substring(1), ">" + read.name.substring(1) + "\n" + read.bases + "\n");
		return records;
	}

	private static Map<String, String> records(Iterable<Read> reads) {
		Map<String, String> records = new TreeMap<String, String>();
		for (Read read : reads)
			records.put(read.name, read.fastq());
		return records;
	}

	/** The highest quality wins and, if it ties, the lowest name. */
	private static boolean isBetter(int qualSum, String name, int thanQualSum, String than) {
		if (qualSum != thanQualSum)
			return qualSum > thanQualSum;
		return name.compareTo(than) < 0;
	}

	/** The mates of a pair of reads. */
	private static final class Pair {
		final Read first;
		final Read second;

		Pair(Read first, Read second) {
			this.first = first;
			this.second = second;
		}

		int qualSum() {
			return first.qualSum() + second.qualSum();
		}

		boolean isBetterThan(Pair than) {
			return isBetter(qualSum(), first.name, than.qualSum(), than.first.name);
		}
	}

	private static String[] prefixes(Random random) {
		String[] prefixes = new String[12];
		for (int i = 0; i < prefixes.length; i++)
			prefixes[i] = randomBases(random, "", PREFIX_LENGTH);
		return prefixes;
	}

	/** The bases with a mismatch in the suffix, at a different position for each copy: variants are at most 2 mismatches apart. */
	private static String variant(String bases, int copy) {
		char[] chars = bases.toCharArray();
		chars[PREFIX_LENGTH + copy] = chars[PREFIX_LENGTH + copy] == 'A' ? 'C' : 'A';
		return new String(chars);
	}

	/**
	 * Groups of 1 to 5 reads that must collapse in one (identical sequences or, with variants, sequences that are
	 * at most 2 mismatches apart), in prefix groups shared by several of them. The reads that must be kept are added to best.
	 */
	private static List<Read> singleEndReads(Random random, int groups, boolean variants, List<Read> best) {
		List<Read> reads = new ArrayList<Read>();
		String[] prefixes = prefixes(random);
		int next = 0;

		for (int g = 0; g < groups; g++) {
			String bases = randomBases(random, prefixes[random.nextInt(prefixes.length)], READ_LENGTH);
			Read kept = null;

			for (int copy = 0, n = 1 + random.nextInt(5); copy < n; copy++) {
				Read read = new Read(String.format("@r%05d", next++), variants && copy > 0 ? variant(bases, copy) : bases, randomQuals(random, READ_LENGTH));
				reads.add(read);
				if (kept == null || isBetter(read.qualSum(), read.name, kept.qualSum(), kept.name))
					kept = read;
			}
			best.add(kept);
		}

		Collections.shuffle(reads, random);
		return reads;
	}

	/**
	 * Pairs in groups like the single-end ones (the variants have their mismatch in the first mate), plus, for some of
	 * them, a pair with the same first mate and a different second mate, which is not a duplicate.
	 */
	private static List<Pair> pairedEndReads(Random random, int groups, boolean variants, List<Pair> best) {
		List<Pair> pairs = new ArrayList<Pair>();
		String[] prefixes = prefixes(random);
		int next = 0;

		for (int g = 0; g < groups; g++) {
			String bases1 = randomBases(random, prefixes[random.nextInt(prefixes.length)], READ_LENGTH);
			String[] bases2 = { randomBases(random, "", READ_LENGTH), randomBases(random, "", READ_LENGTH) };

			for (int mate2 = 0; mate2 < (random.nextInt(3) == 0 ? 2 : 1); mate2++) {
				Pair kept = null;

				for (int copy = 0, n = 1 + random.nextInt(5); copy < n; copy++) {
					String name = String.format("@p%05d", next++);
					Pair pair = new Pair(new Read(name, variants && copy > 0 ? variant(bases1, copy) : bases1, randomQuals(random, READ_LENGTH)),
							new Read(name, bases2[mate2], randomQuals(random, READ_LENGTH)));
					pairs.add(pair);
					if (kept == null || pair.isBetterThan(kept))
						kept = pair;
				}
				best.add(kept);
			}
		}

		Collections.shuffle(pairs, random);
		return pairs;
	}

	private static List<Read> firsts(List<Pair> pairs) {
		List<Read> reads = new ArrayList<Read>();
		for (Pair pair : pairs)
			reads.add(pair.first);
		return reads;
	}

	private static List<Read> seconds(List<Pair> pairs) {
		List<Read> reads = new ArrayList<Read>();
		for (Pair pair : pairs)
			reads.add(pair.second);
		return reads;
	}

	private void checkSingleEnd(boolean variants, int mismatches, boolean combiner) throws Exception {
		List<Read> best = new ArrayList<Read>();
		List<Read> reads = singleEndReads(new Random(1), 1500, variants, best);
		File input = write("reads.fastq", reads);
		assertTrue("too small to be split", input.length() > 4 * SPLIT_SIZE);
		writeConf(true, combiner);

		assertEquals(0, run("-i", input.getAbsolutePath(), "-m", String.valueOf(mismatches), "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(best), output("output1"));
	}

	private void checkPairedEnd(boolean mapJoin, boolean variants, int mismatches, boolean combiner) throws Exception {
		List<Pair> best = new ArrayList<Pair>();
		List<Pair> pairs = pairedEndReads(new Random(2), 1000, variants, best);
		File input1 = write("reads_1.fastq", firsts(pairs));
		File input2 = write("reads_2.fastq", seconds(pairs));
		assertTrue("too small to be split", input1.length() > 4 * SPLIT_SIZE);
		writeConf(mapJoin, combiner);

		assertEquals(0, run("-i", input1.getAbsolutePath(), "-p", input2.getAbsolutePath(), "-m", String.valueOf(mismatches), "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(firsts(best)), output("output1"));
		assertEquals(records(seconds(best)), output("output2"));
	}

	/** The two files of a pair are compressed with bzip2, which is splittable: the mates must still be read together. */
	private void checkPairedEndBzip2(boolean mapJoin) throws Exception {
		List<Pair> best = new ArrayList<Pair>();
		List<Pair> pairs = pairedEndReads(new Random(9), 1000, false, best);
		File input1 = writeBzip2("reads_1.fastq.bz2", firsts(pairs));
		File input2 = writeBzip2("reads_2.fastq.bz2", seconds(pairs));
		assertTrue("too small to be split", input1.length() > 4 * SPLIT_SIZE && input2.length() > 4 * SPLIT_SIZE);
		assertTrue("the files must not compress to the same size", input1.length() != input2.length());
		writeConf(mapJoin, false);

		assertEquals(0, run("-i", input1.getAbsolutePath(), "-p", input2.getAbsolutePath(), "-m", "0", "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(firsts(best)), output("output1"));
		assertEquals(records(seconds(best)), output("output2"));
	}

	@Test
	public void pairedEndMapSideJoinOfBzip2FilesKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEndBzip2(true);
	}

	@Test
	public void pairedEndReduceSideJoinOfBzip2FilesKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEndBzip2(false);
	}

	@Test
	public void singleEndWithoutMismatchesKeepsTheBestReadOfEachSequence() throws Exception {
		checkSingleEnd(false, 0, false);
	}

	@Test
	public void singleEndWithMismatchesKeepsTheBestReadOfEachGroupOfSimilarReads() throws Exception {
		checkSingleEnd(true, 2, false);
	}

	@Test
	public void singleEndWithTheInMapperCombinerKeepsTheBestReadOfEachSequence() throws Exception {
		checkSingleEnd(false, 0, true);
	}

	@Test
	public void pairedEndMapSideJoinWithTheInMapperCombinerKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(true, false, 0, true);
	}

	@Test
	public void pairedEndReduceSideJoinWithTheInMapperCombinerKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(false, false, 0, true);
	}

	@Test
	public void pairedEndMapSideJoinKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(true, false, 0, false);
	}

	@Test
	public void pairedEndReduceSideJoinKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(false, false, 0, false);
	}

	@Test
	public void pairedEndWithMismatchesKeepsTheBestPairOfEachGroupOfSimilarPairs() throws Exception {
		checkPairedEnd(true, true, 2, false);
	}

	@Test
	public void fastaSingleEndKeepsTheReadWithTheLowestNameOfEachSequence() throws Exception {
		List<Read> reads = singleEndReads(new Random(4), 1500, false, new ArrayList<Read>());
		File input = writeFasta("reads.fasta", reads);
		assertTrue("too small to be split", input.length() > 4 * SPLIT_SIZE);
		writeConf(true, false);

		assertEquals(0, run("-i", input.getAbsolutePath(), "-m", "0", "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-f"));

		assertEquals(lowestNameOfEachSequence(reads), fastaOutput("output1"));
	}

	@Test
	public void fastaPairedEndKeepsTheLowestNamedPairOfEachDuplicate() throws Exception {
		List<Pair> pairs = pairedEndReads(new Random(5), 1000, false, new ArrayList<Pair>());
		File input1 = writeFasta("reads_1.fasta", firsts(pairs));
		File input2 = writeFasta("reads_2.fasta", seconds(pairs));
		assertTrue("too small to be split", input1.length() > 4 * SPLIT_SIZE);
		writeConf(true, false);

		assertEquals(0, run("-i", input1.getAbsolutePath(), "-p", input2.getAbsolutePath(), "-m", "0", "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-f"));

		// A pair is a duplicate if both mates are
		Map<String, Pair> lowest = new HashMap<String, Pair>();
		for (Pair pair : pairs) {
			String key = pair.first.bases + "/" + pair.second.bases;
			Pair kept = lowest.get(key);
			if (kept == null || pair.first.name.compareTo(kept.first.name) < 0)
				lowest.put(key, pair);
		}
		Map<String, String> firsts = new TreeMap<String, String>();
		Map<String, String> seconds = new TreeMap<String, String>();
		for (Pair pair : lowest.values()) {
			String name = ">" + pair.first.name.substring(1);
			firsts.put(name, name + "\n" + pair.first.bases + "\n");
			seconds.put(name, name + "\n" + pair.second.bases + "\n");
		}

		assertEquals(firsts, fastaOutput("output1"));
		assertEquals(seconds, fastaOutput("output2"));
	}

	/** Reads of thousands of bases (FASTA, so that they tie and the lowest name wins): groups of copies of 1 to 3 reads. */
	private static List<Read> longReads(Random random, int groups, int[] lengths, boolean variants) {
		List<Read> reads = new ArrayList<Read>();
		String[] prefixes = prefixes(random);
		int next = 0;

		for (int g = 0; g < groups; g++) {
			String bases = randomBases(random, prefixes[random.nextInt(prefixes.length)], lengths[g % lengths.length]);

			for (int copy = 0, n = 1 + random.nextInt(3); copy < n; copy++) {
				// With -c the bases after the compared ones do not matter: the copies differ only there
				char[] chars = bases.toCharArray();
				if (variants && copy > 0)
					chars[chars.length - copy] = chars[chars.length - copy] == 'A' ? 'C' : 'A';
				reads.add(new Read(String.format("@long%05d", next++), new String(chars), ""));
			}
		}

		Collections.shuffle(reads, random);
		return reads;
	}

	@Test
	public void longReadsAreDeduplicated() throws Exception {
		List<Read> reads = longReads(new Random(7), 30, new int[] { 3000, 40000 }, false);
		File input = writeFasta("long.fasta", reads);
		writeConf(true, false);

		assertEquals(0, run("-i", input.getAbsolutePath(), "-m", "0", "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-f"));

		assertEquals(lowestNameOfEachSequence(reads), fastaOutput("output1"));
	}

	@Test
	public void longReadsAreComparedOnlyUpToTheNumberOfBasesToCompare() throws Exception {
		List<Read> reads = longReads(new Random(8), 30, new int[] { 3000, 40000 }, true);
		File input = writeFasta("long.fasta", reads);
		writeConf(true, false);

		// Larger than a short. The variants differ in the last bases, which are not compared
		assertEquals(0, run("-i", input.getAbsolutePath(), "-m", "0", "-l", String.valueOf(PREFIX_LENGTH), "-c", "2000", "-nr", String.valueOf(SLOTS), "-f"));

		Map<String, String> kept = fastaOutput("output1");
		Map<String, Read> lowest = new HashMap<String, Read>();
		for (Read read : reads) {
			String key = read.bases.length() + read.bases.substring(0, 2000);
			Read current = lowest.get(key);
			if (current == null || read.name.compareTo(current.name) < 0)
				lowest.put(key, read);
		}
		Map<String, String> expected = new TreeMap<String, String>();
		for (Read read : lowest.values())
			expected.put(">" + read.name.substring(1), ">" + read.name.substring(1) + "\n" + read.bases + "\n");

		assertEquals(expected, kept);
	}

	private static final int OPTICAL_DIST = 20;

	/** The names of the reads in the style of Casava (the coordinates are in the first word) or of the SRA (in the second). */
	private static String opticalName(boolean sra, int id, int tile, int x, int y) {
		if (sra)
			return String.format("@SRR000001.%d INST:10:FC:1:%d:%d:%d/1", id, tile, x, y);
		return String.format("@INST:10:FC:1:%d:%d:%d 1:N:0:ACGT", tile, x, y);
	}

	/**
	 * Groups of reads with the same sequence (or at most 2 mismatches apart) in spots of two tiles. The spots of a group are
	 * far away from each other and the reads of a spot are all close to each other (the pairs, with the first mate).
	 * A kept read is the best one of a spot, whatever the order of arrival. The list best has them.
	 */
	private static List<Pair> opticalPairs(Random random, boolean sra, boolean paired, int groups, boolean variants, List<List<Pair>> spots, List<Pair> best) {
		List<Pair> pairs = new ArrayList<Pair>();
		String[] prefixes = prefixes(random);
		int id = 0;

		for (int g = 0; g < groups; g++) {
			String bases1 = randomBases(random, prefixes[random.nextInt(prefixes.length)], READ_LENGTH);
			String bases2 = randomBases(random, "", READ_LENGTH);

			for (int tile = 1101; tile <= 1102; tile++) {
				for (int spot = 0, n = 1 + random.nextInt(3); spot < n; spot++) {
					// The spots are at least 4 times the distance apart, the reads of a spot are within half of it of its centre
					int cx = 1000 + 4 * OPTICAL_DIST * spot + 10000 * g;
					int cy = 2000 + 7 * OPTICAL_DIST * spot;
					List<Pair> inSpot = new ArrayList<Pair>();
					Set<String> positions = new HashSet<String>();
					Pair kept = null;

					for (int copy = 0, copies = 1 + random.nextInt(3); copy < copies; ) {
						int x = cx + random.nextInt(15) - 7;
						int y = cy + random.nextInt(15) - 7;
						if (!positions.add(x + ":" + y))
							continue;

						String name = opticalName(sra, id++, tile, x, y);
						Pair pair = new Pair(new Read(name, variants && copy > 0 ? variant(bases1, copy) : bases1, randomQuals(random, READ_LENGTH)),
								new Read(name, bases2, randomQuals(random, READ_LENGTH)));
						pairs.add(pair);
						inSpot.add(pair);
						copy++;

						if (kept == null || (paired ? pair.isBetterThan(kept) : isBetter(pair.first.qualSum(), pair.first.name, kept.first.qualSum(), kept.first.name)))
							kept = pair;
					}

					best.add(kept);
					spots.add(inSpot);
				}
			}
		}

		Collections.shuffle(pairs, random);
		return pairs;
	}

	private void checkOptical(boolean sra, boolean variants, int mismatches, boolean combiner) throws Exception {
		List<Pair> best = new ArrayList<Pair>();
		List<Pair> pairs = opticalPairs(new Random(10), sra, false, 150, variants, new ArrayList<List<Pair>>(), best);
		File input = write("optical.fastq", firsts(pairs));
		assertTrue("too small to be split", input.length() > 4 * SPLIT_SIZE);
		writeConf(true, combiner);

		assertEquals(0, run("-i", input.getAbsolutePath(), "-m", String.valueOf(mismatches), "-d", String.valueOf(OPTICAL_DIST), "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(firsts(best)), output("output1"));
	}

	@Test
	public void singleEndWithAnOpticalDistanceKeepsTheBestReadOfEachSpot() throws Exception {
		checkOptical(false, false, 0, false);
	}

	@Test
	public void singleEndWithAnOpticalDistanceAndTheCoordinatesInTheSecondWordOfTheName() throws Exception {
		checkOptical(true, false, 0, false);
	}

	@Test
	public void singleEndWithAnOpticalDistanceAndMismatchesKeepsTheBestReadOfEachSpot() throws Exception {
		checkOptical(false, true, 2, false);
	}

	private void checkOpticalPaired(boolean mapJoin, boolean sra) throws Exception {
		List<Pair> best = new ArrayList<Pair>();
		List<Pair> pairs = opticalPairs(new Random(11), sra, true, 120, false, new ArrayList<List<Pair>>(), best);
		File input1 = write("optical_1.fastq", firsts(pairs));
		File input2 = write("optical_2.fastq", seconds(pairs));
		assertTrue("too small to be split", input1.length() > 4 * SPLIT_SIZE);
		writeConf(mapJoin, false);

		assertEquals(0, run("-i", input1.getAbsolutePath(), "-p", input2.getAbsolutePath(), "-m", "0", "-d", String.valueOf(OPTICAL_DIST), "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(firsts(best)), output("output1"));
		assertEquals(records(seconds(best)), output("output2"));
	}

	@Test
	public void pairedEndMapSideJoinWithAnOpticalDistanceKeepsTheBestPairOfEachSpot() throws Exception {
		checkOpticalPaired(true, false);
	}

	@Test
	public void pairedEndReduceSideJoinWithAnOpticalDistanceKeepsTheBestPairOfEachSpot() throws Exception {
		checkOpticalPaired(false, false);
	}

	@Test
	public void pairedEndWithAnOpticalDistanceAndTheCoordinatesInTheSecondWordOfTheName() throws Exception {
		checkOpticalPaired(true, true);
	}

	@Test
	public void singleEndWithAnOpticalDistanceAndTheInMapperCombinerKeepsTheBestReadOfEachSpot() throws Exception {
		checkOptical(false, false, 0, true);
	}

	@Test
	public void aReadWithoutCoordinatesMakesTheRunFailIfThereIsAnOpticalDistance() throws Exception {
		List<Read> reads = singleEndReads(new Random(3), 20, false, new ArrayList<Read>());
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0", "-d", "10"));

		assertEquals(-1, output.status);
	}

	@Test
	public void anOpticalDistanceLowerThanOneIsAdjustedWithAWarning() throws Exception {
		List<Pair> pairs = opticalPairs(new Random(13), false, false, 10, false, new ArrayList<List<Pair>>(), new ArrayList<Pair>());
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("optical.fastq", firsts(pairs)), "-m", "0", "-d", "0"));

		assertEquals(0, output.status);
		assertTrue(output.events.toString(), output.logged(Level.WARN, "-d 0"));
	}

	private List<Read> readsWithComments() {
		List<Read> reads = new ArrayList<Read>();
		Random random = new Random(6);
		for (int i = 0; i < 20; i++)
			reads.add(new Read(String.format("@r%05d lane:%d length=%d", i, i % 3, READ_LENGTH), randomBases(random, "", READ_LENGTH), randomQuals(random, READ_LENGTH)));
		return reads;
	}

	@Test
	public void theReadNamesAreKeptWhole() throws Exception {
		List<Read> reads = readsWithComments();
		writeConf(true, false);

		assertEquals(0, run(singleEndArgs(write("reads.fastq", reads), "-m", "0")));

		assertEquals(records(reads), output("output1"));
	}

	@Test
	public void theReadNamesAreCutAtTheFirstSpaceIfTheParserTrimsThem() throws Exception {
		List<Read> reads = readsWithComments();
		List<Read> trimmed = new ArrayList<Read>();
		for (Read read : reads)
			trimmed.add(new Read(read.name.split(" ")[0], read.bases, read.quals));
		jobProperties.put("hsp.trim.sequence.name", "true");
		writeConf(true, false);

		assertEquals(0, run(singleEndArgs(write("reads.fastq", reads), "-m", "0")));

		assertEquals(records(trimmed), output("output1"));
	}

	/** What a run of the tool wrote to the standard output and error and logged (INFO or higher). */
	private static final class Output {
		int status;
		String stdout;
		String stderr;
		final List<LoggingEvent> events = new ArrayList<LoggingEvent>();

		boolean logged(Level level, String text) {
			for (LoggingEvent event : events)
				if (event.getLevel().equals(level) && event.getRenderedMessage().contains(text))
					return true;
			return false;
		}
	}

	private Output runCapturing(String... args) throws Exception {
		final Output output = new Output();
		Logger logger = Logger.getLogger("es.udc.gac.mardre");
		Level level = logger.getLevel();
		boolean additive = logger.getAdditivity();
		PrintStream stdout = System.out;
		PrintStream stderr = System.err;
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();
		AppenderSkeleton appender = new AppenderSkeleton() {
			@Override
			protected void append(LoggingEvent event) {
				synchronized (output.events) {
					output.events.add(event);
				}
			}

			@Override
			public boolean requiresLayout() {
				return false;
			}

			@Override
			public void close() {}
		};

		logger.setLevel(Level.INFO);
		logger.setAdditivity(false);
		logger.addAppender(appender);
		System.setOut(new PrintStream(bytes, true, "UTF-8"));
		System.setErr(new PrintStream(errorBytes, true, "UTF-8"));
		try {
			output.status = run(args);
		} finally {
			System.setOut(stdout);
			System.setErr(stderr);
			logger.removeAppender(appender);
			logger.setAdditivity(additive);
			logger.setLevel(level);
		}
		output.stdout = bytes.toString("UTF-8");
		output.stderr = errorBytes.toString("UTF-8");
		return output;
	}

	private String[] singleEndArgs(File input, String... extra) {
		List<String> args = new ArrayList<String>(Arrays.asList("-i", input.getAbsolutePath(), "-l", String.valueOf(PREFIX_LENGTH), "-nr", "2", "-q"));
		args.addAll(Arrays.asList(extra));
		return args.toArray(new String[0]);
	}

	@Test
	public void theSummaryIsPrintedToTheStandardOutput() throws Exception {
		List<Read> best = new ArrayList<Read>();
		List<Read> reads = singleEndReads(new Random(3), 200, false, best);
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(0, output.status);
		assertTrue(output.stdout, output.stdout.contains(String.format("MarDRe: non duplicate reads %d/%d (", best.size(), reads.size())));
		assertTrue(output.stdout, output.stdout.contains("MarDRe: execution time was "));
		assertTrue(output.stdout, output.stdout.contains("Duplicate reads were removed in "));
		assertTrue(output.stdout, output.stdout.contains("MarDRe: output path = "));
	}

	@Test
	public void theDiagnosticsOfTheDriverAreLoggedInsteadOfPrinted() throws Exception {
		List<Read> reads = singleEndReads(new Random(3), 200, false, new ArrayList<Read>());
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(0, output.status);
		for (String diagnostic : new String[] { "prefix length", "running in single-end mode", "removing duplicate reads" })
			assertFalse(diagnostic + " printed:\n" + output.stdout, output.stdout.contains(diagnostic));
		assertTrue(output.logged(Level.INFO, "prefix length = " + PREFIX_LENGTH));
		assertTrue(output.logged(Level.INFO, "running in single-end mode"));
	}

	@Test
	public void theStatisticsOfTheTasksAreLoggedInsteadOfPrinted() throws Exception {
		List<Read> reads = singleEndReads(new Random(3), 200, false, new ArrayList<Read>());
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(0, output.status);
		for (String statistic : new String[] { "Total time for", "Setup time", "Write time", "reads in " })
			assertFalse(statistic + " printed:\n" + output.stdout, output.stdout.contains(statistic));
		assertTrue(output.logged(Level.INFO, "Mapper task"));
		assertTrue(output.logged(Level.INFO, "Reducer task"));
	}

	@Test
	public void aFailedJobIsLoggedAsAnError() throws Exception {
		// 1 is not a letter: the mappers fail
		List<Read> reads = Arrays.asList(new Read("@bad", "ACGTACGT1CGTACGTACGTACGTACGTACGTACGTACGT", "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"));
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(-1, output.status);
		assertTrue(output.events.toString(), output.logged(Level.ERROR, "job_local"));
		assertTrue(output.stderr, output.stderr.contains("job_local"));
	}

	@Test
	public void anInvalidConfigurationValueIsLoggedAsAnError() throws Exception {
		List<Read> reads = singleEndReads(new Random(3), 20, false, new ArrayList<Read>());
		writeConf(true, false);
		File conf = new File(home, "etc/mardre.conf");
		Files.write(conf.toPath(), "INPUT_BUFFER_SIZE=10\n".getBytes(StandardCharsets.UTF_8), java.nio.file.StandardOpenOption.APPEND);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(-1, output.status);
		assertTrue(output.events.toString(), output.logged(Level.ERROR, "INPUT_BUFFER_SIZE=10"));
		assertTrue(output.stderr, output.stderr.contains("INPUT_BUFFER_SIZE=10"));
	}

	@Test
	public void theAdjustmentOfAnOptionIsLoggedAsAWarning() throws Exception {
		List<Read> reads = singleEndReads(new Random(3), 20, false, new ArrayList<Read>());
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "-1"));

		assertEquals(0, output.status);
		assertFalse(output.stdout, output.stdout.contains("WARNING"));
		assertTrue(output.events.toString(), output.logged(Level.WARN, "-m -1"));
	}
}
