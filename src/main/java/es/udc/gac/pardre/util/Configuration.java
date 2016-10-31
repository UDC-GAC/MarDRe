package es.udc.gac.pardre.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

	private static final String PARDREMR_DEFAULT_CONFIGURATION_FILE = "/pardre-mr.conf";
	private static Properties properties;
	public static boolean MERGE = true; 
	public static boolean DELETE_TEMP = true; 

	public static void loadConfiguration() throws FileNotFoundException
	{
			InputStream is = Configuration.class.getClassLoader().getResourceAsStream(PARDREMR_DEFAULT_CONFIGURATION_FILE);
			
			try {
				properties.load(is);
				is.close();
			} catch (IOException e) {
				throw new FileNotFoundException();
			}

			MERGE = Boolean.parseBoolean(properties.getProperty("mergeOutput"));
			DELETE_TEMP = Boolean.parseBoolean(properties.getProperty("deleteTemp"));

			if (MERGE == false)
				DELETE_TEMP = false;
	}
}