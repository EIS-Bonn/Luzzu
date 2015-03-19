package de.unibonn.iai.eis.luzzu.properties;

/**
 * @author Jeremy Debattista
 *
 * This class makes visible all variable properties in
 * the framework
 */
public class EnvironmentProperties {

	protected static String datasetURI = "";
	private static EnvironmentProperties instance = null;
	
	protected EnvironmentProperties(){	}
	
	public static EnvironmentProperties getInstance(){
		if (instance == null) instance = new EnvironmentProperties();
		return instance;
	}

	/**
	 * Returns the dataset URI being processed.
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public String getDatasetURI() throws IllegalStateException {
		if (PropertyManager.getInstance().environmentVars.containsKey("datasetURI")){
			return PropertyManager.getInstance().environmentVars.get("datasetURI");
		} else {
			throw new IllegalStateException("Dataset is not Set");
		}
	}
	
	/**
	 * Sets the dataset URI
	 * @param datasetURI
	 */
	public void setDatasetURI(String datasetURI){
		PropertyManager.getInstance().environmentVars.put("datasetURI", datasetURI);
	}
	
	/**
	 * Returns the base URI being processed.
	 * @throws Exception if the process is not initialised and dataset is not known
	 */
	public String getBaseURI() throws IllegalStateException {
		if (PropertyManager.getInstance().environmentVars.containsKey("baseURI")){
			return PropertyManager.getInstance().environmentVars.get("baseURI");
		} else {
			throw new IllegalStateException("Processor is not initialised yet");
		}
	}
}
