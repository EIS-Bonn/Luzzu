package de.unibonn.iai.eis.luzzu.properties;

import java.io.IOException;
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
	protected ConcurrentMap<String, String> environmentVars = new ConcurrentHashMap<String, String>();
	
	protected PropertyManager(){
		try{
			Properties prop = new Properties();
			prop.load(getClass().getResourceAsStream("/properties/cache.properties"));
			propMap.put("cache.properties", prop);	
			
			prop = new Properties();
			prop.load(getClass().getResourceAsStream("/properties/webservice.properties"));
			propMap.put("webservice.properties", prop);
			
			prop = new Properties();
			prop.load(getClass().getResourceAsStream("/properties/spark.properties"));
			propMap.put("spark.properties", prop);
		} catch (IOException ex)  {
			//TODO: good exception handling
			ex.printStackTrace();
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
	
	/**
	 * Adds an environment variable value
	 * 
	 * @param key - Variable's name
	 * @param value - Variable's value
	 */
	public void addToEnvironmentVars(String key, String value){
		this.environmentVars.put(key, value);
	}
}