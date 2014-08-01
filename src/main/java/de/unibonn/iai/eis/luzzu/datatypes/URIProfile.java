package de.unibonn.iai.eis.luzzu.datatypes;

import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;


public class URIProfile {

	@Deprecated  private Node uriNode;
	@Deprecated private int httpStatusCode = 0;
	@Deprecated private boolean isValidDereferencableURI = false;
	private String uri;
	private boolean isBroken = false;
	private Status uriStatus;
	
	private Set<String> structuredContentType = new HashSet<String>(); 

	@Deprecated 
	public Node getUriNode() {
		return uriNode;
	}
	@Deprecated 
	public void setUriNode(Node uri) {
		this.uriNode = uri;
	}
	@Deprecated
	public int getHttpStatusCode() {
		return httpStatusCode;
	}
	@Deprecated
	public void setHttpStatusCode(int httpStatusCode) {
		this.httpStatusCode = httpStatusCode;
	}
	@Deprecated
	public boolean isValidDereferencableURI() {
		return isValidDereferencableURI;
	}
	@Deprecated
	public void setValidDereferencableURI(boolean isValidDereferencableURI) {
		this.isValidDereferencableURI = isValidDereferencableURI;
	}
	public boolean isBroken() {
		return isBroken;
	}
	public void setBroken(boolean isBroken) {
		this.isBroken = isBroken;
	}
	public Set<String> getStructuredContentType() {
		return structuredContentType;
	}
	public void addToStructuredContentType(String contentType) {
		this.structuredContentType.add(contentType);
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public Status getUriStatus() {
		return uriStatus;
	}
	public void setUriStatus(Status uriStatus) {
		this.uriStatus = uriStatus;
	}

	
}
