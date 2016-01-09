package de.unibonn.iai.eis.luzzu.io.helper;

public class IOStats {

	private String className;
	private Long triplesProcessed;
	
	
	public IOStats(String className, Long triplesProcessed){
		this.className = className;
		this.triplesProcessed = triplesProcessed;
	}
	
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	/**
	 * @param className the className to set
	 */
	public void setClassName(String className) {
		this.className = className;
	}
	/**
	 * @return the triplesProcessed
	 */
	public Long getTriplesProcessed() {
		return triplesProcessed;
	}
	/**
	 * @param triplesProcessed the triplesProcessed to set
	 */
	public void setTriplesProcessed(Long triplesProcessed) {
		this.triplesProcessed = triplesProcessed;
	}
}
