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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.udc.gac.mardre.InMapperCombiner;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

public final class Configuration {

	private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

	public static final String MARDRE_VERSION = "1.4";
	public static final String MARDRE_WEBPAGE = "http://mardre.des.udc.es";

	// MarDRe configuration (madre.conf)
	private static final String MARDRE_CONFIG_FILE = "mardre.conf";
	private static final Properties MARDRE_PROPERTIES = new Properties();
	public static boolean MERGE;
	public static boolean DELETE_TEMP;
	public static boolean PAIRED_END_MAP_JOIN;
	public static boolean COMPRESS_MAP_OUTPUT;
	public static boolean COMPRESS_INTERMEDIATE_OUTPUT;
	public static int INPUT_BUFFER_SIZE;
	public static boolean IN_MAPPER_COMBINER;
	public static int IN_MAPPER_COMBINER_CACHE_SIZE;
	public static int CLUSTER_LIST_INITIAL_CAPACITY;
	public static short HDFS_BLOCK_REPLICATION;
	public static String HDFS_BASE_PATH;
	public static short PREFIX_LENGTH;
	public static short MISMATCH;
	public static int NUMBER_BASES_COMPARED;
	public static FileFormat FILE_FORMAT = FileFormat.FILE_FORMAT_FASTQ;
	public static String INPUT_PATH_1;

	public static String OUTPUT_PATH;
	public static String TEMP_INPUT_PATH_1;
	public static String TEMP_OUTPUT_PATH_1;
	public static String TEMP_OUTPUT_PATH_2;
	public static String TEMP_OUTPUT_PATH_2_PREFIX;

	private static final String IN_MAPPER_COMBINER_PARAM = "MarDRe_inMapperCombiner";
	private static final String IN_MAPPER_COMBINER_CACHE_SIZE_PARAM = "MarDRe_inMapperCombinerCacheSize";
	private static final String CLUSTER_LIST_INITIAL_CAPACITY_PARAM = "MarDRe_clusterListInitialCapacity";
	private static final String PREFIX_LENGTH_PARAM = "MarDRe_prefixLength";
	private static final String MISMATCH_PARAM = "MarDRe_misMatch";
	private static final String NUMBER_BASES_COMPARED_PARAM = "MarDRe_numBasesCompared";
	private static final String FILE_FORMAT_PARAM = "MarDRe_fileFormat";
	private static final String INPUT_PATH_1_PARAM = "MarDRe_inputPath1";
	private static final String TEMP_OUTPUT_PATH_2_PREFIX_PARAM = "MarDRe_tempOutputPath2Prefix";

	/**
	 * Returns the installation directory of MarDRe. Only the client needs it (to read mardre.conf):
	 * the tasks get their settings from the job configuration.
	 */
	public static String getHome() {
		String home = System.getenv("MARDRE_HOME");

		if (home == null)
			throw new RuntimeException("'MARDRE_HOME' must be set");

		return home;
	}

	private Configuration() {}

	public static boolean load(FileSystem fs, org.apache.hadoop.conf.Configuration jobConf, Options options, Path inputPath1, FileFormat format) throws IOException {

		String slash = System.getProperty("file.separator");
		String configFileName = getHome()+slash+"etc"+slash+MARDRE_CONFIG_FILE;

		try {
			FileInputStream configFile = new FileInputStream(configFileName);
			MARDRE_PROPERTIES.load(configFile);
			configFile.close();
		} catch (IOException e) {
			throw new FileNotFoundException("File: "+configFileName);
		}

		MERGE = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "MERGE_OUTPUT", "false"));
		DELETE_TEMP = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "DELETE_TEMP", "true"));
		PAIRED_END_MAP_JOIN = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "PAIRED_END_MAP_JOIN", "true"));
		COMPRESS_MAP_OUTPUT = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "COMPRESS_MAP_OUTPUT", "false"));
		COMPRESS_INTERMEDIATE_OUTPUT = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "COMPRESS_INTERMEDIATE_OUTPUT", "false"));
		INPUT_BUFFER_SIZE = Integer.parseInt(Configuration.getProperty(MARDRE_PROPERTIES, "INPUT_BUFFER_SIZE", "65536"));
		IN_MAPPER_COMBINER = Boolean.parseBoolean(Configuration.getProperty(MARDRE_PROPERTIES, "IN_MAPPER_COMBINER", "false"));
		IN_MAPPER_COMBINER_CACHE_SIZE = Integer.parseInt(Configuration.getProperty(MARDRE_PROPERTIES, "IN_MAPPER_COMBINER_CACHE_SIZE", InMapperCombiner.CACHE_SIZE.toString()));
		CLUSTER_LIST_INITIAL_CAPACITY = Integer.parseInt(Configuration.getProperty(MARDRE_PROPERTIES, "CLUSTER_LIST_INITIAL_CAPACITY", "65536"));
		HDFS_BLOCK_REPLICATION = Short.parseShort(Configuration.getProperty(MARDRE_PROPERTIES, "HDFS_BLOCK_REPLICATION", "1"));
		Path basePath = new Path(Configuration.getProperty(MARDRE_PROPERTIES, "HDFS_BASE_PATH", fs.getHomeDirectory().toString()));

		if (!basePath.equals(fs.getHomeDirectory())) {
			if (!fs.exists(basePath)) {
				Errors.report(LOG, "HDFS_BASE_PATH value is not valid: {} does not exist", basePath);
				return false;
			}

			if(!fs.isDirectory(basePath)) {
				Errors.report(LOG, "HDFS_BASE_PATH value is not valid: {} is not a directory", basePath);
				return false;
			}
		} else {
			if (!fs.exists(basePath))
				fs.mkdirs(basePath);
		}

		HDFS_BASE_PATH = fs.resolvePath(basePath).toString();
		String concatPath = HDFS_BASE_PATH;

		if (basePath.isRoot())
			concatPath = concatPath.substring(0, concatPath.length()-1);

		OUTPUT_PATH = concatPath+slash+"MarDRe"+slash+"output";
		TEMP_INPUT_PATH_1 = concatPath+slash+"MarDRe"+slash+"tmp"+slash+"input1";
		TEMP_OUTPUT_PATH_1 = concatPath+slash+"MarDRe"+slash+"tmp"+slash+"output1";
		TEMP_OUTPUT_PATH_2 = concatPath+slash+"MarDRe"+slash+"tmp"+slash+"output2";
		TEMP_OUTPUT_PATH_2_PREFIX = TEMP_OUTPUT_PATH_2+slash+"part";

		if (MERGE == false)
			DELETE_TEMP = false;

		if (INPUT_BUFFER_SIZE < 512) {
			Errors.report(LOG, "INPUT_BUFFER_SIZE={} value is not valid", INPUT_BUFFER_SIZE);
			return false;
		}

		if (IN_MAPPER_COMBINER_CACHE_SIZE <= 0) {
			Errors.report(LOG, "IN_MAPPER_COMBINER_CACHE_SIZE={} value is not valid", IN_MAPPER_COMBINER_CACHE_SIZE);
			return false;
		}

		if (CLUSTER_LIST_INITIAL_CAPACITY < 0) {
			Errors.report(LOG, "CLUSTER_LIST_INITIAL_CAPACITY={} value is not valid", CLUSTER_LIST_INITIAL_CAPACITY);
			return false;
		}

		if (HDFS_BLOCK_REPLICATION <= 0) {
			Errors.report(LOG, "HDFS_BLOCK_REPLICATION={} value is not valid", HDFS_BLOCK_REPLICATION);
			return false;
		}

		// Configure input buffer size for HSP library
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(jobConf, INPUT_BUFFER_SIZE);
		// Configure MarDRe paramaters
		jobConf.setBoolean(IN_MAPPER_COMBINER_PARAM, IN_MAPPER_COMBINER);
		jobConf.setInt(IN_MAPPER_COMBINER_CACHE_SIZE_PARAM, IN_MAPPER_COMBINER_CACHE_SIZE);
		jobConf.setInt(CLUSTER_LIST_INITIAL_CAPACITY_PARAM, CLUSTER_LIST_INITIAL_CAPACITY);
		jobConf.setInt(PREFIX_LENGTH_PARAM, options.getPrefixLength());
		jobConf.setInt(MISMATCH_PARAM, options.getMisMatch());
		jobConf.setInt(NUMBER_BASES_COMPARED_PARAM, options.getNumberOfBasesCompared());
		jobConf.setInt(FILE_FORMAT_PARAM, format.ordinal());
		jobConf.set(INPUT_PATH_1_PARAM, inputPath1.toString());
		jobConf.set(TEMP_OUTPUT_PATH_2_PREFIX_PARAM, TEMP_OUTPUT_PATH_2_PREFIX);

		return true;
	}

	public static void set(org.apache.hadoop.conf.Configuration jobConf) {
		IN_MAPPER_COMBINER = Boolean.parseBoolean(jobConf.get(IN_MAPPER_COMBINER_PARAM));
		IN_MAPPER_COMBINER_CACHE_SIZE = Integer.parseInt(jobConf.get(IN_MAPPER_COMBINER_CACHE_SIZE_PARAM));
		CLUSTER_LIST_INITIAL_CAPACITY = Integer.parseInt(jobConf.get(CLUSTER_LIST_INITIAL_CAPACITY_PARAM));
		PREFIX_LENGTH = Short.parseShort(jobConf.get(PREFIX_LENGTH_PARAM));
		MISMATCH = Short.parseShort(jobConf.get(MISMATCH_PARAM));
		NUMBER_BASES_COMPARED = jobConf.getInt(NUMBER_BASES_COMPARED_PARAM, -1);
		FILE_FORMAT = FileFormat.values()[Integer.parseInt(jobConf.get(FILE_FORMAT_PARAM))];
		INPUT_PATH_1 = jobConf.get(INPUT_PATH_1_PARAM);
		TEMP_OUTPUT_PATH_2_PREFIX = jobConf.get(TEMP_OUTPUT_PATH_2_PREFIX_PARAM);
	}

	public static final int fastABS(int value) {
		// Very fast ABS value computation for integers
		int mask = value >> 31;
		return (value + mask) ^ mask;
	}

	private static String getProperty(Properties properties, String key, String defaultValue) {
		String value = properties.getProperty(key, defaultValue);

		if (value.isEmpty())
			return defaultValue;

		return value;
	}
}
