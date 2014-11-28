package de.unibonn.iai.eis.luzzu.operations.datatypes;


public class RankedElement implements Comparable<RankedElement> {

	private String datasetURI;
	private float totalRankValue;
	
	public RankedElement(String datasetURI, float totalRankValue){
		this.setDatasetURI(datasetURI);
		this.setTotalRankValue(totalRankValue);
	}

	public String getDatasetURI() {
		return datasetURI;
	}

	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}

	public float getTotalRankValue() {
		return totalRankValue;
	}

	public void setTotalRankValue(float totalRankValue) {
		this.totalRankValue = totalRankValue;
	}

	public int compareTo(RankedElement o) {
		if (this.totalRankValue < o.getTotalRankValue()) return -1;
		if (this.totalRankValue > o.getTotalRankValue()) return 1;
		else return 0;
	}


	
	
}
