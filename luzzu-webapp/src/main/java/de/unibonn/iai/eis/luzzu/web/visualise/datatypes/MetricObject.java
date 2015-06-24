package de.unibonn.iai.eis.luzzu.web.visualise.datatypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class MetricObject {
	private String inDimension = "";
	private String inCategory = "";
	private String name = "";
	private String uri = "";
	private Set<String> commonDatasets = new HashSet<String>();
	private Double latestValue = 0.0d;
	private List<ObservationObject> lstObservations = new ArrayList<ObservationObject>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public Set<String> getCommonDatasets() {
		return commonDatasets;
	}
	public void setCommonDatasets(Set<String> commonDatasets) {
		this.commonDatasets = commonDatasets;
	}
	public Double getLatestValue() {
		return latestValue;
	}
	public void setLatestValue(Double latestValue) {
		this.latestValue = latestValue;
	}
	public String getInDimension() {
		return inDimension;
	}
	public void setInDimension(String inDimension) {
		this.inDimension = inDimension;
	}
	public String getInCategory() {
		return inCategory;
	}
	public void setInCategory(String inCategory) {
		this.inCategory = inCategory;
	}
	public List<ObservationObject> getLstObservations() {
		return lstObservations;
	}
	public void setLstObservations(List<ObservationObject> lstObservations) {
		this.lstObservations = lstObservations;
	}
}
