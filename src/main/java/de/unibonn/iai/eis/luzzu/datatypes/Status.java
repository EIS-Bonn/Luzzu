package de.unibonn.iai.eis.luzzu.datatypes;



/**
 * @author Jeremy Debatista
 * 
 * This class represent the Status (RequestURI, StatusCode, TemporaryURI)
 * required to Dereference URI
 * 
 * @see <a href="http://dl.dropboxusercontent.com/u/4138729/paper/dereference_iswc2011.pdf">
 * Dereferencing Semantic Web URIs: What is 200 OK on the Semantic Web?</a> 
 * 
 */
public class Status {
	private String uri;
	private StatusCode sc;
	private String turi;
	
	
	/**
	 * @return The Request URI
	 */
	public String getUri() {
		return uri;
	}
	
	/**
	 * @param Sets the Request URI
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	/**
	 * @return The StatusCode of the Requested URI
	 */
	public StatusCode getStatusCode() {
		return sc;
	}
	
	/**
	 * @param Sets the StatusCode of the Requested URI
	 */
	public void setStatusCode(StatusCode sc) {
		this.sc = sc;
	}
	
	/**
	 * Redirected Request URIs from previous response
	 * @return The Temporary URI.
	 */
	public String getTuri() {
		return turi;
	}
	
	
	/**
	 * @param Sets the Temporary URI
	 */
	public void setTuri(String turi) {
		this.turi = turi;
	}
}
