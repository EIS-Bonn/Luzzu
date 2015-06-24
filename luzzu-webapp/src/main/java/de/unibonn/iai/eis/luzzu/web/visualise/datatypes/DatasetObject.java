package de.unibonn.iai.eis.luzzu.web.visualise.datatypes;

import java.util.HashSet;
import java.util.Set;


public class DatasetObject {
	private String name = "";
	private Set<MetricObject> metrics = new HashSet<MetricObject>();
	
	public Set<MetricObject> getMetrics() {
		return metrics;
	}
	public void setMetrics(Set<MetricObject> metrics) {
		this.metrics = metrics;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
