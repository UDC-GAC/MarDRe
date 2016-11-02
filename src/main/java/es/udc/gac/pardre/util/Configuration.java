package es.udc.gac.pardre.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map;

public class Configuration {

	public static String PARDREMR_HOME = null;
	public static boolean MERGE = true; 
	public static boolean DELETE_TEMP = true;
	public static boolean PAIRED_JOIN = true;
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

	public static void loadConfiguration() throws FileNotFoundException
	{
			try {
				FileInputStream configFile = new FileInputStream(configFileName);
				properties.load(configFile);
				configFile.close();
			} catch (IOException e) {
				throw new FileNotFoundException();
			}

			MERGE = Boolean.parseBoolean(properties.getProperty("mergeOutput"));
			DELETE_TEMP = Boolean.parseBoolean(properties.getProperty("deleteTemp"));
			PAIRED_JOIN = Boolean.parseBoolean(properties.getProperty("pairedJoin"));

			if (MERGE == false)
				DELETE_TEMP = false;
	}
}
