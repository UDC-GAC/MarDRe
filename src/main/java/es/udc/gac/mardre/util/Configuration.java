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

import es.udc.gac.mardre.InMapperCombiner;
import es.udc.gac.mardre.util.SequenceParserFactory.FileFormat;

import java.util.Map;

public class Configuration {

	public static String MARDRE_HOME = null;
	public static boolean MERGE; 
	public static boolean DELETE_TEMP;
	public static boolean PAIRED_JOIN;
	public static boolean COMPRESS_TEMP;
	public static int IO_FILE_BUFFER_SIZE;
	public static boolean IN_MAPPER_COMBINER;
	public static int IN_MAPPER_COMBINER_CACHE_SIZE;

	public static short prefixLength;
	public static short missMatch;
	public static FileFormat fileFormat;
	public static String inputPath1;

	public static final String IO_BUFFER_SIZE_PARAM = "MarDRe_ioBufferSize";
	public static final String IN_MAPPER_COMBINER_PARAM = "MarDRe_inMapperCombiner";
	public static final String IN_MAPPER_COMBINER_CACHE_SIZE_PARAM = "MarDRe_inMapperCombinerCacheSize";
	public static final String PREFIXLENGTH_PARAM = "MarDRe_prefixLength";
	public static final String MISSMATCH_PARAM = "MarDRe_missMatch";
	public static final String FILE_FORMAT_PARAM = "MarDRe_fileFormat";
	public static final String INPUT_PATH_1_PARAM = "MarDRe_inputPath1";

	public static final String TEMP_INPUT_PATH_1 = "/MarDRe/tmp/input1";
	public static final String TEMP_INPUT_PATH_2 = "/MarDRe/tmp/input2";
	public static final String TEMP_OUTPUT_PATH_1 = "/MarDRe/tmp/output1";
	public static final String TEMP_OUTPUT_PATH_2 = "/MarDRe/tmp/output2";
	public static final String OUTPUT_PATH_2 = Configuration.TEMP_OUTPUT_PATH_2+"/part";
	private static final String MARDRE_CONFIGURATION_FILE = "mardre.conf";
	private static String configFileName = null;
	private static Properties properties = new Properties();

	static {
		Map<String,String> map = System.getenv();
		MARDRE_HOME = map.get("MARDRE_HOME");

		if(MARDRE_HOME == null)
			throw new RuntimeException("'MARDRE_HOME' must be set");

		String slash = System.getProperty("file.separator");
		configFileName = MARDRE_HOME+slash+"etc"+slash+MARDRE_CONFIGURATION_FILE;
	}

	public static void loadConfiguration() throws FileNotFoundException {
		try {
			FileInputStream configFile = new FileInputStream(configFileName);
			properties.load(configFile);
			configFile.close();
		} catch (IOException e) {
			throw new FileNotFoundException("File: "+configFileName);
		}

		MERGE = Boolean.parseBoolean(properties.getProperty("mergeOutput", "true"));
		DELETE_TEMP = Boolean.parseBoolean(properties.getProperty("deleteTemp", "true"));
		PAIRED_JOIN = Boolean.parseBoolean(properties.getProperty("pairedJoin", "false"));
		COMPRESS_TEMP = Boolean.parseBoolean(properties.getProperty("compressTemp", "false"));
		IO_FILE_BUFFER_SIZE = Integer.parseInt(properties.getProperty("ioBufferSize", "65536"));
		IN_MAPPER_COMBINER = Boolean.parseBoolean(properties.getProperty("inMapperCombiner", "false"));
		IN_MAPPER_COMBINER_CACHE_SIZE = Integer.parseInt(properties.getProperty("inMapperCombinerCacheSize", InMapperCombiner.CACHE_SIZE.toString()));

		if (MERGE == false)
			DELETE_TEMP = false;
	}

	public static void setParameters(org.apache.hadoop.conf.Configuration jobConf) {
		prefixLength = Short.parseShort(jobConf.get(Configuration.PREFIXLENGTH_PARAM));
		missMatch = Short.parseShort(jobConf.get(Configuration.MISSMATCH_PARAM));
		fileFormat = FileFormat.values()[Integer.parseInt(jobConf.get(Configuration.FILE_FORMAT_PARAM))];
		inputPath1 = jobConf.get(Configuration.INPUT_PATH_1_PARAM);
	}
}
