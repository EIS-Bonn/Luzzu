package de.unibonn.iai.eis.luzzu.io.helper;

public class IOStats {

	private String className = "";
	private Long triplesProcessed = 0l;
	private boolean doneParsing = false;
	
	private String qmdStatus = "";
	private String qrStatus = "";
	
	
	public IOStats(String className, Long triplesProcessed){
		this.className = className;
		this.triplesProcessed = triplesProcessed;
	}
	
	public IOStats(String className, Long triplesProcessed, Boolean doneParsing){
		this.className = className;
		this.triplesProcessed = triplesProcessed;
		this.setDoneParsing(doneParsing);
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

	/**
	 * @return the doneParsing
	 */
	public boolean isDoneParsing() {
		return doneParsing;
	}

	/**
	 * @param doneParsing the doneParsing to set
	 */
	public void setDoneParsing(boolean doneParsing) {
		this.doneParsing = doneParsing;
	}

	/**
	 * @return the qmdStatus
	 */
	public String getQmdStatus() {
		return qmdStatus;
	}

	/**
	 * @param qmdStatus the qmdStatus to set
	 */
	public void setQmdStatus(String qmdStatus) {
		this.qmdStatus = qmdStatus;
	}

	/**
	 * @return the qrStatus
	 */
	public String getQrStatus() {
		return qrStatus;
	}

	/**
	 * @param qrStatus the qrStatus to set
	 */
	public void setQrStatus(String qrStatus) {
		this.qrStatus = qrStatus;
	}
}
