package de.unibonn.iai.eis.luzzu.semantics.datatypes;

import java.util.Date;

import com.hp.hpl.jena.rdf.model.Resource;

public class Observation implements Comparable<Observation> {

	private Resource observationURI;
	private Date dateComputed;
	private Double value;
	private Resource metricType;
	private Resource computedOn;
	private Resource graphURI;

	public Observation(Resource observationURI, Date dateComputed, Double value, Resource metricType, Resource computedOn, Resource graphURI){
		this.setObservationURI(observationURI);
		this.setDateComputed(dateComputed);
		this.setValue(value);
		this.setMetricType(metricType);
		this.setComputedOn(computedOn);
		this.setGraphURI(graphURI);
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

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
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

	/**
	 * @return the computedOn
	 */
	public Resource getComputedOn() {
		return computedOn;
	}

	/**
	 * @param computedOn the computedOn to set
	 */
	public void setComputedOn(Resource computedOn) {
		this.computedOn = computedOn;
	}

	/**
	 * @return the graphURI
	 */
	public Resource getGraphURI() {
		return graphURI;
	}

	/**
	 * @param graphURI the graphURI to set
	 */
	public void setGraphURI(Resource graphURI) {
		this.graphURI = graphURI;
	}
}
