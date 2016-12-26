/*
 * Copyright (C) 2017 Universidade da Coruña
 * 
 * This file is part of MaRDRe.
 * 
 * MaRDRe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * MaRDRe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with MaRDRe.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.mardre.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import es.udc.gac.mardre.InMapperCombiner;

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

	public static final String IO_BUFFER_SIZE_PARAM = "MaRDRe_ioBufferSize";
	public static final String IN_MAPPER_COMBINER_PARAM = "MaRDRe_inMapperCombiner";
	public static final String IN_MAPPER_COMBINER_CACHE_SIZE_PARAM = "MaRDRe_inMapperCombinerCacheSize";
	public static final String PREFIXLENGTH = "MaRDRe_prefixLength";
	public static final String MINQUAL = "MaRDRe_minQual";
	public static final String MISSMATCH = "MaRDRe_missMatch";
	public static final String FILE_FORMAT = "MaRDRe_fileFormat";
	public static final String INPUT_PATH_1 = "MaRDRePath1";
	public static final String TEMP_INPUT_PATH_1 = "/MaRDRe/tmp/input1";
	public static final String TEMP_INPUT_PATH_2 = "/MaRDRe/tmp/input2";
	public static final String TEMP_OUTPUT_PATH_1 = "/MaRDRe/tmp/output1";
	public static final String TEMP_OUTPUT_PATH_2 = "/MaRDRe/tmp/output2";

	private static String configFileName = null;
	private static final String MARDRE_CONFIGURATION_FILE = "mardre.conf";
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
}
