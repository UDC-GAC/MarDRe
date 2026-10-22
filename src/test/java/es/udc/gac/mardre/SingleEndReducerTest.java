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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.util.Progress;
import org.junit.Test;

import es.udc.gac.mardre.util.FastQSeqStringParser;
import es.udc.gac.mardre.util.SeqStringParser;
import es.udc.gac.mardre.util.Sequence;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

/**
 * Runs the whole reducer task (run(), as Hadoop does) over groups of reads that
 * are serialized and deserialized like the shuffle does, so the value object
 * is reused between the reads of a group.
 */
public class SingleEndReducerTest {

	private static final short PREFIX_LENGTH = 5;
	private static final String BASES = "ACGTACGTACGTACGTACGTACGT";
	private static final SeqStringParser PARSER = new FastQSeqStringParser(PREFIX_LENGTH, (short) -1);

	/** A read with a sequence and a constant quality score. */
	private static final class Read {
		final String name;
		final String bases;
		final char qual;

		Read(String name, String bases, char qual) {
			this.name = name;
			this.bases = bases;
			this.qual = qual;
		}

		String fastq() {
			char[] quals = new char[bases.length()];
			Arrays.fill(quals, qual);
			return name + "\n" + bases + "\n+\n" + new String(quals);
		}
	}

	private static Configuration jobConf(int mismatch) {
		Configuration conf = new Configuration(false);
		// Names of the parameters set by es.udc.gac.mardre.util.Configuration.load()
		conf.setBoolean("MarDRe_inMapperCombiner", false);
		conf.setInt("MarDRe_inMapperCombinerCacheSize", 1);
		conf.setInt("MarDRe_clusterListInitialCapacity", 16);
		conf.setInt("MarDRe_prefixLength", PREFIX_LENGTH);
		conf.setInt("MarDRe_misMatch", mismatch);
		conf.setInt("MarDRe_numBasesCompared", -1);
		conf.setInt("MarDRe_fileFormat", FileFormat.FILE_FORMAT_FASTQ.ordinal());
		conf.set("MarDRe_inputPath1", "in");
		conf.set("MarDRe_tempOutputPath2Prefix", "out2");
		return conf;
	}

	private static byte[] serialize(org.apache.hadoop.io.Writable w) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		w.write(new DataOutputStream(bytes));
		return bytes.toByteArray();
	}

	/** Shuffle: sort by key keeping the arrival order of the reads that share it. */
	private static RawKeyValueIterator shuffle(List<Read> reads) throws IOException {
		final List<byte[][]> records = new ArrayList<byte[][]>();
		final Map<Long, List<Sequence>> byKey = new java.util.TreeMap<Long, List<Sequence>>();

		for (Read read : reads) {
			Sequence seq = new Sequence();
			PARSER.getSeq(read.fastq(), seq, true);
			List<Sequence> list = byKey.get(seq.getKey());
			if (list == null)
				byKey.put(seq.getKey(), list = new ArrayList<Sequence>());
			list.add(seq);
		}

		for (Map.Entry<Long, List<Sequence>> e : byKey.entrySet())
			for (Sequence seq : e.getValue())
				records.add(new byte[][] { serialize(new LongWritable(e.getKey())), serialize(seq) });

		return new RawKeyValueIterator() {
			private int pos = -1;
			private final DataInputBuffer key = new DataInputBuffer();
			private final DataInputBuffer value = new DataInputBuffer();

			public boolean next() {
				if (++pos >= records.size())
					return false;
				key.reset(records.get(pos)[0], records.get(pos)[0].length);
				value.reset(records.get(pos)[1], records.get(pos)[1].length);
				return true;
			}

			public DataInputBuffer getKey() { return key; }
			public DataInputBuffer getValue() { return value; }
			public Progress getProgress() { return new Progress(); }
			public void close() {}
		};
	}

	/** Runs the reducer task and returns the FASTQ records written (name, bases and qualities). */
	private static List<String> reduce(List<Read> reads, int mismatch) throws Exception {
		final List<String> output = new ArrayList<String>();
		Configuration conf = jobConf(mismatch);
		TaskAttemptID id = new TaskAttemptID("test", 1, TaskType.REDUCE, 0, 0);
		Counters counters = new Counters();
		Counter keys = counters.findCounter("test", "keys");
		Counter values = counters.findCounter("test", "values");

		RecordWriter<NullWritable, Sequence> writer = new RecordWriter<NullWritable, Sequence>() {
			@Override
			public void write(NullWritable key, Sequence seq) {
				// The reducer reuses the objects it writes: keep their current content
				output.add(seq.toString());
			}

			@Override
			public void close(TaskAttemptContext context) {}
		};

		StatusReporter reporter = new StatusReporter() {
			public Counter getCounter(Enum<?> name) { return new Counters().findCounter(name); }
			public Counter getCounter(String group, String name) { return new Counters().findCounter(group, name); }
			public void progress() {}
			public float getProgress() { return 0; }
			public void setStatus(String status) {}
		};

		ReduceContextImpl<LongWritable, Sequence, NullWritable, Sequence> reduceContext = new ReduceContextImpl<LongWritable, Sequence, NullWritable, Sequence>(
				conf, id, shuffle(reads), keys, values, writer, (OutputCommitter) null, reporter,
				WritableComparator.get(LongWritable.class), LongWritable.class, Sequence.class);
		Reducer<LongWritable, Sequence, NullWritable, Sequence>.Context context = new WrappedReducer<LongWritable, Sequence, NullWritable, Sequence>()
				.getReducerContext(reduceContext);

		new SingleEndReducer().run(context);
		return output;
	}

	/** The name of each record written (the first line of its FASTQ text). */
	private static List<String> names(List<String> records) {
		List<String> names = new ArrayList<String>();
		for (String record : records)
			names.add(record.substring(0, record.indexOf('\n')));
		Collections.sort(names);
		return names;
	}

	private static String arrival(List<Read> reads) {
		StringBuilder sb = new StringBuilder();
		for (Read read : reads)
			sb.append(read.name).append(' ');
		return sb.toString().trim();
	}

	private static List<List<Read>> permutations(List<Read> reads) {
		List<List<Read>> result = new ArrayList<List<Read>>();
		permute(new ArrayList<Read>(reads), 0, result);
		return result;
	}

	private static void permute(List<Read> reads, int k, List<List<Read>> result) {
		if (k == reads.size()) {
			result.add(new ArrayList<Read>(reads));
			return;
		}
		for (int i = k; i < reads.size(); i++) {
			Collections.swap(reads, k, i);
			permute(reads, k + 1, result);
			Collections.swap(reads, k, i);
		}
	}

	@Test
	public void distinctReadsAreAllKept() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@a", "ACGTAAAAAAAAAAAAAAAAAAAA", '5'),
				new Read("@b", "ACGTACCCCCCCCCCCCCCCCCCC", '5'),
				new Read("@c", "ACGTAGGGGGGGGGGGGGGGGGGG", '5'));

		assertEquals(Arrays.asList("@a", "@b", "@c"), names(reduce(reads, 0)));
	}

	@Test
	public void duplicatesKeepTheReadWithTheHighestQualityWhateverTheArrivalOrder() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@low", BASES, '5'),
				new Read("@mid", BASES, '?'),
				new Read("@high", BASES, 'I'));

		for (List<Read> order : permutations(reads))
			assertEquals("arrival order " + arrival(order), Arrays.asList("@high"), names(reduce(order, 0)));
	}

	@Test
	public void aBetterDuplicateOfTheFirstReadReplacesIt() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@first", BASES, '5'),
				new Read("@better", BASES, 'I'));

		assertEquals(Arrays.asList("@better"), names(reduce(reads, 0)));
	}

	@Test
	public void aWorseDuplicateOfTheFirstReadIsDropped() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@first", BASES, 'I'),
				new Read("@worse", BASES, '5'));

		assertEquals(Arrays.asList("@first"), names(reduce(reads, 0)));
	}

	@Test
	public void duplicatesOfSeveralReadsOfTheSameGroupAreResolvedIndependently() throws Exception {
		String other = "ACGTAGGGGGGGGGGGGGGGGGGG";
		List<Read> reads = Arrays.asList(
				new Read("@a1", BASES, '5'),
				new Read("@b1", other, '5'),
				new Read("@a2", BASES, 'I'),
				new Read("@b2", other, '?'),
				new Read("@a3", BASES, '?'));

		for (List<Read> order : permutations(reads))
			assertEquals("arrival order " + arrival(order), Arrays.asList("@a2", "@b2"), names(reduce(order, 0)));
	}

	@Test
	public void duplicatesWithTheSameQualityKeepTheLowestNameWhateverTheArrivalOrder() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@c", BASES, '5'),
				new Read("@a", BASES, '5'),
				new Read("@b", BASES, '5'));

		for (List<Read> order : permutations(reads))
			assertEquals("arrival order " + arrival(order), Arrays.asList("@a"), names(reduce(order, 0)));
	}

	@Test
	public void theNameOnlyDecidesBetweenReadsWithTheBestQuality() throws Exception {
		List<Read> reads = Arrays.asList(
				new Read("@a", BASES, '5'),
				new Read("@z", BASES, 'I'),
				new Read("@m", BASES, 'I'),
				new Read("@b", BASES, '?'));

		for (List<Read> order : permutations(reads))
			assertEquals("arrival order " + arrival(order), Arrays.asList("@m"), names(reduce(order, 0)));
	}

	/** Few quality levels, so there are many ties: the kept read of each sequence does not depend on the arrival order. */
	@Test
	public void randomGroupsWithTiesKeepTheBestQualityAndTheLowestName() throws Exception {
		Random random = new Random(7);
		String[] variants = new String[4];
		for (int i = 0; i < variants.length; i++) {
			StringBuilder sb = new StringBuilder("ACGTA");
			for (int j = 0; j < 19; j++)
				sb.append("ACGT".charAt(random.nextInt(4)));
			variants[i] = sb.toString();
		}

		for (int round = 0; round < 200; round++) {
			List<Read> reads = new ArrayList<Read>();
			Map<String, Read> best = new HashMap<String, Read>();

			for (int i = 0; i < 2 + random.nextInt(14); i++) {
				Read read = new Read("@r" + i, variants[random.nextInt(variants.length)], "5?I".charAt(random.nextInt(3)));
				reads.add(read);

				Read current = best.get(read.bases);
				if (current == null || read.qual > current.qual || (read.qual == current.qual && read.name.compareTo(current.name) < 0))
					best.put(read.bases, read);
			}

			List<String> expected = new ArrayList<String>();
			for (Read read : best.values())
				expected.add(read.name);
			Collections.sort(expected);

			assertEquals("round " + round + ", arrival order " + arrival(reads), expected, names(reduce(reads, 0)));
		}
	}

	/**
	 * With mismatches, the distances of the reads kept so far are relative to the first read of the group: they
	 * must not change when a better duplicate replaces it, or later duplicates would be compared with a wrong filter.
	 */
	@Test
	public void theDistanceReferenceDoesNotChangeWhenABetterDuplicateReplacesTheFirstRead() throws Exception {
		String first = "ACGTAAAAAAAAAAAAAAAAAAAA";
		// 2 mismatches with first
		String better = "ACGTACCAAAAAAAAAAAAAAAAA";
		// 4 mismatches with first
		String other = "ACGTAAAAAAGGGGAAAAAAAAAA";
		// 1 mismatch with other (5 with first, 7 with better)
		String otherDup = "ACGTAAAAAAGGGGAAAAATAAAA";

		List<Read> reads = Arrays.asList(
				new Read("@first", first, '5'),
				new Read("@other", other, '5'),
				new Read("@better", better, 'I'),
				new Read("@otherDup", otherDup, '?'));

		assertEquals(Arrays.asList("@better", "@otherDup"), names(reduce(reads, 2)));
	}

	/** Without mismatches the kept reads are the distinct sequences, each with the best quality of its duplicates. */
	@Test
	public void randomGroupsMatchTheBestQualityOfEachDistinctSequence() throws Exception {
		Random random = new Random(42);
		String[] variants = new String[6];
		for (int i = 0; i < variants.length; i++) {
			StringBuilder sb = new StringBuilder("ACGTA");
			for (int j = 0; j < 19; j++)
				sb.append("ACGT".charAt(random.nextInt(4)));
			variants[i] = sb.toString();
		}

		for (int round = 0; round < 200; round++) {
			List<Read> reads = new ArrayList<Read>();
			Map<String, Character> best = new HashMap<String, Character>();

			for (int i = 0; i < 2 + random.nextInt(14); i++) {
				String bases = variants[random.nextInt(variants.length)];
				// Distinct quality for every read, so there are no ties
				char qual = (char) ('!' + i * 3);
				reads.add(new Read("@r" + i, bases, qual));
				if (!best.containsKey(bases) || best.get(bases) < qual)
					best.put(bases, qual);
			}

			Map<String, Character> kept = new LinkedHashMap<String, Character>();
			for (String record : reduce(reads, 0)) {
				String[] lines = record.split("\n");
				assertEquals("duplicated output sequence in round " + round, null, kept.put(lines[1], lines[3].charAt(0)));
			}

			assertEquals("round " + round, best, kept);
		}
	}
}
