package de.unibonn.iai.eis.luzzu.operations.ranking;

import com.hp.hpl.jena.rdf.model.Resource;

public class RankingConfiguration {

	private Resource uriResource;
	private RankBy type;
	private double weight;
	
	public RankingConfiguration(Resource uriResource, RankBy type, double weight) {
		this.uriResource = uriResource;
		this.type = type;
		this.weight = weight;
	}
	
	public Resource getUriResource() {
		return uriResource;
	}
	public void setUriResource(Resource uriResource) {
		this.uriResource = uriResource;
	}
	public RankBy getType() {
		return type;
	}
	public void setType(RankBy type) {
		this.type = type;
	}
	public double getWeight() {
		return weight;
	}
	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	
}
