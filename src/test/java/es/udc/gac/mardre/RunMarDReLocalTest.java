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
			List<String> lines = Files.readAllLines(part.toPath(), StandardCharsets.UTF_8);
			assertEquals(0, lines.size() % 4);
			for (int i = 0; i < lines.size(); i += 4) {
				String record = lines.get(i) + "\n" + lines.get(i + 1) + "\n" + lines.get(i + 2) + "\n" + lines.get(i + 3) + "\n";
				assertEquals("duplicated name " + lines.get(i), null, records.put(lines.get(i), record));
			}
		}
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

		Map<String, String> kept = output("output1");

		if (!combiner) {
			assertEquals(records(best), kept);
			return;
		}

		// The in-mapper combiner keeps the first read of a sequence that a mapper sees, whatever its quality
		Map<String, Set<String>> namesOfSequence = new HashMap<String, Set<String>>();
		for (Read read : reads) {
			if (!namesOfSequence.containsKey(read.bases))
				namesOfSequence.put(read.bases, new HashSet<String>());
			namesOfSequence.get(read.bases).add(read.name);
		}

		Set<String> sequences = new HashSet<String>();
		for (Map.Entry<String, String> record : kept.entrySet()) {
			String bases = record.getValue().split("\n")[1];
			assertTrue("a read of another sequence: " + record.getKey(), namesOfSequence.get(bases).contains(record.getKey()));
			assertTrue("sequence kept twice: " + bases, sequences.add(bases));
		}
		assertEquals(namesOfSequence.keySet(), sequences);
	}

	private void checkPairedEnd(boolean mapJoin, boolean variants, int mismatches) throws Exception {
		List<Pair> best = new ArrayList<Pair>();
		List<Pair> pairs = pairedEndReads(new Random(2), 1000, variants, best);
		File input1 = write("reads_1.fastq", firsts(pairs));
		File input2 = write("reads_2.fastq", seconds(pairs));
		assertTrue("too small to be split", input1.length() > 4 * SPLIT_SIZE);
		writeConf(mapJoin, false);

		assertEquals(0, run("-i", input1.getAbsolutePath(), "-p", input2.getAbsolutePath(), "-m", String.valueOf(mismatches), "-l", String.valueOf(PREFIX_LENGTH), "-nr", String.valueOf(SLOTS), "-q"));

		assertEquals(records(firsts(best)), output("output1"));
		assertEquals(records(seconds(best)), output("output2"));
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
	public void singleEndWithTheInMapperCombinerKeepsOneReadOfEachSequence() throws Exception {
		checkSingleEnd(false, 0, true);
	}

	@Test
	public void pairedEndMapSideJoinKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(true, false, 0);
	}

	@Test
	public void pairedEndReduceSideJoinKeepsTheBestPairOfEachDuplicate() throws Exception {
		checkPairedEnd(false, false, 0);
	}

	@Test
	public void pairedEndWithMismatchesKeepsTheBestPairOfEachGroupOfSimilarPairs() throws Exception {
		checkPairedEnd(true, true, 2);
	}

	/** What a run of the tool wrote to the standard output and logged (INFO or higher). */
	private static final class Output {
		int status;
		String stdout;
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
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
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
		try {
			output.status = run(args);
		} finally {
			System.setOut(stdout);
			logger.removeAppender(appender);
			logger.setAdditivity(additive);
			logger.setLevel(level);
		}
		output.stdout = bytes.toString("UTF-8");
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
	public void aFailedJobIsLoggedAsAnError() throws Exception {
		// 1 is not a letter: the mappers fail
		List<Read> reads = Arrays.asList(new Read("@bad", "ACGTACGT1CGTACGTACGTACGTACGTACGTACGTACGT", "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII"));
		writeConf(true, false);

		Output output = runCapturing(singleEndArgs(write("reads.fastq", reads), "-m", "0"));

		assertEquals(-1, output.status);
		assertTrue(output.events.toString(), output.logged(Level.ERROR, "job_local"));
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
