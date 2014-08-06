package de.unibonn.iai.eis.luzzu.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Jeremy Debattista
 * 
 * Handles the loading and storing of the defined 
 * properties found in src/main/resources/properties 
 * folder.
 */
public class PropertyManager {

	private static PropertyManager instance = null;
	private ConcurrentMap<String,Properties> propMap = new ConcurrentHashMap<String,Properties>();
	
	protected PropertyManager(){
		Properties prop = new Properties();
		InputStream input = null;

		try{
			URL propertiesFolder = getClass().getClassLoader().getResource("properties");
			File folder = new File(propertiesFolder.toString());
			File[] listOfFiles = folder.listFiles();
			
			for(File propertyFile : listOfFiles){
				input = new FileInputStream(propertyFile);
				prop.load(input);
				propMap.put(propertyFile.getName(), prop);
			}
		} catch (IOException ex)  {
			//TODO: good exception handling
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				// TODO good exception handling
			}
		}
	}
	
	/**
	 * Create or return a singleton instance of the PropertyManager
	 * 
	 * @return PropertyManager instance
	 */
	public static PropertyManager getInstance(){
		if (instance == null) instance = new PropertyManager();
		return instance;
	}
	
	/**
	 * Returns the properties of a particular configuration.
	 * @param propertiesRequired - The file name (e.g. cache.properties) for the requried configuration.
	 * @return The properties for a configuration.
	 */
	public Properties getProperties(String propertiesRequired){
		return this.propMap.get(propertiesRequired);
	}
}
