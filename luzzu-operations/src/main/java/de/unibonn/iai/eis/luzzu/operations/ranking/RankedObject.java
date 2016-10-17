package de.unibonn.iai.eis.luzzu.operations.ranking;



public class RankedObject implements Comparable<RankedObject>{

	private String dataset;
	private double rankedValue;
	private String graphUri;
	
	public RankedObject(String dataset, double rankedValue, String graphURI){
		this.setDataset(dataset);
		this.setRankedValue(rankedValue);
		this.setGraphUri(graphURI);
	}
	
	public String getDataset() {
		return dataset;
	}
	public void setDataset(String dataset) {
		this.dataset = dataset;
	}
	public double getRankedValue() {
		return rankedValue;
	}
	public void setRankedValue(double rankedValue) {
		this.rankedValue = rankedValue;
	}
	
	@Override
	public int compareTo(RankedObject o) {
		if (this.rankedValue < o.getRankedValue()) return -1;
		if (this.rankedValue > o.getRankedValue()) return 1;
		else return 0;
	}
	
	@Override
	public boolean equals(Object other){
		if (other instanceof RankedObject){
			RankedObject _other = (RankedObject) other;
			return this.dataset.equals(_other.dataset);
		} else return false;
	}
	
	@Override
	public int hashCode(){
		return this.dataset.hashCode();
	}

	public String getGraphUri() {
		return graphUri;
	}

	public void setGraphUri(String graphUri) {
		this.graphUri = graphUri;
	}
	
}
