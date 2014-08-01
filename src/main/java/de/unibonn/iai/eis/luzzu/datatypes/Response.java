package de.unibonn.iai.eis.luzzu.datatypes;

/**
 * @author Jeremy Debatista
 * 
 * This class represent the Response (StatusCode, URI)
 * required to Dereference URI
 * 
 * @see <a href="http://dl.dropboxusercontent.com/u/4138729/paper/dereference_iswc2011.pdf">
 * Dereferencing Semantic Web URIs: What is 200 OK on the Semantic Web?</a> 
 * 
 */
public class Response {
	private StatusCode sc;
	private String uri;
	
	/**
	 * @return The StatusCode of the requested URI's Response
	 */
	public StatusCode getStatusCode() {
		return sc;
	}
	
	/**
	 * @param Sets the StatusCode of the Response
	 */
	public void setStatusCode(StatusCode sc) {
		this.sc = sc;
	}
	
	/**
	 * @return The Response URI if any (This is only for redirection 3XX status code)
	 */
	public String getUri() {
		return uri;
	}
	
	/**
	 * @param Sets the Response URI
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
}
