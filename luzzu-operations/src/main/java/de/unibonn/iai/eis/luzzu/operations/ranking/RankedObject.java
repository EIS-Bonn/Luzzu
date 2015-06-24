package de.unibonn.iai.eis.luzzu.operations.ranking;



public class RankedObject implements Comparable<RankedObject>{

	private String dataset;
	private double rankedValue;
	
	public RankedObject(String dataset, double rankedValue){
		this.setDataset(dataset);
		this.setRankedValue(rankedValue);
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

}
