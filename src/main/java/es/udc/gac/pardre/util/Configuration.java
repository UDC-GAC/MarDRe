package es.udc.gac.pardre.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import es.udc.gac.pardre.mapreduce.InMapperCombiner;

import java.util.Map;

public class Configuration {

	public static String PARDREMR_HOME = null;
	public static boolean MERGE; 
	public static boolean DELETE_TEMP;
	public static boolean PAIRED_JOIN;
	public static boolean COMPRESS_TEMP;
	public static int IO_FILE_BUFFER_SIZE;
	public static boolean IN_MAPPER_COMBINER;
	public static int IN_MAPPER_COMBINER_CACHE_SIZE;

	public static final String IO_BUFFER_SIZE_PARAM = "ParDRe-MR_ioBufferSize";
	public static final String IN_MAPPER_COMBINER_PARAM = "ParDRe-MR_inMapperCombiner";
	public static final String IN_MAPPER_COMBINER_CACHE_SIZE_PARAM = "ParDRe-MR_inMapperCombinerCacheSize";
	public static final String PREFIXLENGTH = "ParDRe-MR_prefixLength";
	public static final String MINQUAL = "ParDRe-MR_minQual";
	public static final String MISSMATCH = "ParDRe-MR_missMatch";
	public static final String FILE_FORMAT = "ParDRe-MR_fileFormat";
	public static final String INPUT_PATH_1 = "ParDRe-MR_inputPath1";
	public static final String TEMP_INPUT_PATH_1 = "/ParDRe-MR/tmp/input1";
	public static final String TEMP_INPUT_PATH_2 = "/ParDRe-MR/tmp/input2";
	public static final String TEMP_OUTPUT_PATH_1 = "/ParDRe-MR/tmp/output1";
	public static final String TEMP_OUTPUT_PATH_2 = "/ParDRe-MR/tmp/output2";

	private static String configFileName = null;
	private static final String PARDREMR_CONFIGURATION_FILE = "pardre-mr.conf";
	private static Properties properties = new Properties();

	static {
		Map<String,String> map = System.getenv();
		PARDREMR_HOME = map.get("PARDREMR_HOME");

		if(PARDREMR_HOME == null)
			throw new RuntimeException("'PARDREMR_HOME' must be set");

		String slash = System.getProperty("file.separator");
		configFileName = PARDREMR_HOME+slash+"etc"+slash+PARDREMR_CONFIGURATION_FILE;
	}

	public static void loadConfiguration() throws FileNotFoundException {
		try {
			FileInputStream configFile = new FileInputStream(configFileName);
			properties.load(configFile);
			configFile.close();
		} catch (IOException e) {
			throw new FileNotFoundException();
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
