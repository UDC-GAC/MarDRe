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
 * along with MarDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import es.udc.gac.mardre.InMapperCombiner;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

import java.util.Map;

public final class Configuration {

	public static final String MARDRE_VERSION = "1.2";
	public static final String TEMP_INPUT_PATH_1 = "/MarDRe/tmp/input1";
	public static final String TEMP_INPUT_PATH_2 = "/MarDRe/tmp/input2";
	public static final String TEMP_OUTPUT_PATH_1 = "/MarDRe/tmp/output1";
	public static final String TEMP_OUTPUT_PATH_2 = "/MarDRe/tmp/output2";
	public static final String OUTPUT_PATH_2 = Configuration.TEMP_OUTPUT_PATH_2+"/part";

	// MarDRe configuration (mardre.conf)
	public static String MARDRE_HOME;
	private static String MARDRE_CONFIG_FILE = "mardre.conf";
	private static Properties MARDRE_PROPERTIES;
	public static boolean MERGE; 
	public static boolean DELETE_TEMP;
	public static boolean PAIRED_JOIN;
	public static boolean PAIRED_COMPRESS_TEMP;
	public static int IO_FILE_BUFFER_SIZE;
	public static boolean IN_MAPPER_COMBINER;
	public static int IN_MAPPER_COMBINER_CACHE_SIZE;
	public static int CLUSTER_LIST_INITIAL_CAPACITY;
	public static short PREFIX_LENGTH;
	public static short MISMATCH;
	public static FileFormat FILE_FORMAT;
	public static String INPUT_PATH_1;

	private static final String IO_FILE_BUFFER_SIZE_PARAM = "MarDRe_ioBufferSize";
	private static final String IN_MAPPER_COMBINER_PARAM = "MarDRe_inMapperCombiner";
	private static final String IN_MAPPER_COMBINER_CACHE_SIZE_PARAM = "MarDRe_inMapperCombinerCacheSize";
	private static final String CLUSTER_LIST_INITIAL_CAPACITY_PARAM = "MarDRe_clusterListInitialCapacity";
	private static final String PREFIX_LENGTH_PARAM = "MarDRe_prefixLength";
	private static final String MISMATCH_PARAM = "MarDRe_misMatch";
	private static final String FILE_FORMAT_PARAM = "MarDRe_fileFormat";
	private static final String INPUT_PATH_1_PARAM = "MarDRe_inputPath1";

	static {
		Map<String,String> map = System.getenv();
		MARDRE_HOME = map.get("MARDRE_HOME");

		if(MARDRE_HOME == null)
			throw new RuntimeException("'MARDRE_HOME' must be set");
	}

	private Configuration() {}

	public static boolean load(org.apache.hadoop.conf.Configuration jobConf, Options options, Path inputPath1, FileFormat format) throws FileNotFoundException {

		String slash = System.getProperty("file.separator");
		String configFileName = MARDRE_HOME+slash+"etc"+slash+MARDRE_CONFIG_FILE;
		MARDRE_PROPERTIES = new Properties();

		try {
			FileInputStream configFile = new FileInputStream(configFileName);
			MARDRE_PROPERTIES.load(configFile);
			configFile.close();
		} catch (IOException e) {
			throw new FileNotFoundException("File: "+configFileName);
		}

		MERGE = Boolean.parseBoolean(MARDRE_PROPERTIES.getProperty("MERGE_OUTPUT", "true"));
		DELETE_TEMP = Boolean.parseBoolean(MARDRE_PROPERTIES.getProperty("DELETE_TEMP", "true"));
		PAIRED_JOIN = Boolean.parseBoolean(MARDRE_PROPERTIES.getProperty("PAIRED_JOIN", "false"));
		PAIRED_COMPRESS_TEMP = Boolean.parseBoolean(MARDRE_PROPERTIES.getProperty("PAIRED_COMPRESS_TEMP", "false"));
		IO_FILE_BUFFER_SIZE = Integer.parseInt(MARDRE_PROPERTIES.getProperty("IO_FILE_BUFFER_SIZE", "65536"));
		IN_MAPPER_COMBINER = Boolean.parseBoolean(MARDRE_PROPERTIES.getProperty("IN_MAPPER_COMBINER", "false"));
		IN_MAPPER_COMBINER_CACHE_SIZE = Integer.parseInt(MARDRE_PROPERTIES.getProperty("IN_MAPPER_COMBINER_CACHE_SIZE", InMapperCombiner.CACHE_SIZE.toString()));
		CLUSTER_LIST_INITIAL_CAPACITY = Integer.parseInt(MARDRE_PROPERTIES.getProperty("CLUSTER_LIST_INITIAL_CAPACITY", "65536"));

		if (MERGE == false)
			DELETE_TEMP = false;

		if (IO_FILE_BUFFER_SIZE <= 0) {
			System.err.println("ioBufferSize="+IO_FILE_BUFFER_SIZE+" value is not valid");
			return false;
		}

		if (IN_MAPPER_COMBINER_CACHE_SIZE <= 0) {
			System.err.println("inMapperCombinerCacheSize="+IN_MAPPER_COMBINER_CACHE_SIZE+" value is not valid");
			return false;
		}

		if (CLUSTER_LIST_INITIAL_CAPACITY < 0) {
			System.err.println("clusterInitialCapacity="+CLUSTER_LIST_INITIAL_CAPACITY+" value is not valid");
			return false;
		}

		jobConf.setInt(IO_FILE_BUFFER_SIZE_PARAM, IO_FILE_BUFFER_SIZE);
		jobConf.setBoolean(IN_MAPPER_COMBINER_PARAM, IN_MAPPER_COMBINER);
		jobConf.setInt(IN_MAPPER_COMBINER_CACHE_SIZE_PARAM, IN_MAPPER_COMBINER_CACHE_SIZE);
		jobConf.setInt(CLUSTER_LIST_INITIAL_CAPACITY_PARAM, CLUSTER_LIST_INITIAL_CAPACITY);
		jobConf.setInt(PREFIX_LENGTH_PARAM, options.getPrefixLength());
		jobConf.setInt(MISMATCH_PARAM, options.getMisMatch());
		jobConf.setInt(FILE_FORMAT_PARAM, format.ordinal());
		jobConf.set(INPUT_PATH_1_PARAM, inputPath1.toString());

		return true;
	}

	public static void set(org.apache.hadoop.conf.Configuration jobConf) {
		IO_FILE_BUFFER_SIZE = Integer.parseInt(jobConf.get(Configuration.IO_FILE_BUFFER_SIZE_PARAM));
		IN_MAPPER_COMBINER = Boolean.parseBoolean(jobConf.get(Configuration.IN_MAPPER_COMBINER_PARAM));
		IN_MAPPER_COMBINER_CACHE_SIZE = Integer.parseInt(jobConf.get(Configuration.IN_MAPPER_COMBINER_CACHE_SIZE_PARAM));
		CLUSTER_LIST_INITIAL_CAPACITY = Integer.parseInt(jobConf.get(Configuration.CLUSTER_LIST_INITIAL_CAPACITY_PARAM));
		PREFIX_LENGTH = Short.parseShort(jobConf.get(Configuration.PREFIX_LENGTH_PARAM));
		MISMATCH = Short.parseShort(jobConf.get(Configuration.MISMATCH_PARAM));
		FILE_FORMAT = FileFormat.values()[Integer.parseInt(jobConf.get(Configuration.FILE_FORMAT_PARAM))];
		INPUT_PATH_1 = jobConf.get(Configuration.INPUT_PATH_1_PARAM);
	}

	public static int getIOFileBufferSize(org.apache.hadoop.conf.Configuration jobConf) {
		return Integer.parseInt(jobConf.get(Configuration.IO_FILE_BUFFER_SIZE_PARAM));
	}

	public static final int fastABS(int value) {
		// Very fast ABS value computation for integers
		int mask = value >> 31;
		return (value + mask) ^ mask;
	}
}
