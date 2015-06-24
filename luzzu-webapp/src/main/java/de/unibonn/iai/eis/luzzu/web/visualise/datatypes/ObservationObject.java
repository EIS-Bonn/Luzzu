package de.unibonn.iai.eis.luzzu.web.visualise.datatypes;

import java.util.Date;

public class ObservationObject {
	private Date observationDate;
	private Double observationValue;
	
	public Date getObservationDate() {
		return observationDate;
	}
	
	public void setObservationDate(Date observationDate) {
		this.observationDate = observationDate;
	}
	
	public Double getObservationValue() {
		return observationValue;
	}
	
	public void setObservationValue(Double observationValue) {
		this.observationValue = observationValue;
	}
}
