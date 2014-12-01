package de.unibonn.iai.eis.luzzu.semantics.datatypes;

import java.util.Date;

import com.hp.hpl.jena.rdf.model.Resource;

public class Observation implements Comparable<Observation> {

	private Resource observationURI;
	private Date dateComputed;
	private Float value;
	private Resource metricType;

	public Observation(Resource observationURI, Date dateComputed, Float value, Resource metricType){
		this.setObservationURI(observationURI);
		this.setDateComputed(dateComputed);
		this.setValue(value);
		this.setMetricType(metricType);
	}

	public Resource getObservationURI() {
		return observationURI;
	}

	public void setObservationURI(Resource observationURI) {
		this.observationURI = observationURI;
	}

	public Date getDateComputed() {
		return dateComputed;
	}

	public void setDateComputed(Date dateComputed) {
		this.dateComputed = dateComputed;
	}

	public Float getValue() {
		return value;
	}

	public void setValue(Float value) {
		this.value = value;
	}

	public int compareTo(Observation anotherObservation) {
		if (this.getDateComputed().after(anotherObservation.getDateComputed())) return 1;
		if (this.getDateComputed().before(anotherObservation.getDateComputed())) return -1;
		else return 0;
	}

	public Resource getMetricType() {
		return metricType;
	}

	public void setMetricType(Resource metricType) {
		this.metricType = metricType;
	}
}
