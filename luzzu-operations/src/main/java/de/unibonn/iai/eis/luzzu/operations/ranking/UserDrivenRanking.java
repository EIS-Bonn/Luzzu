package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.ModelFactory;
import de.unibonn.iai.eis.luzzu.operations.datatypes.RankedElement;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;

public class UserDrivenRanking {
	
	private List<RankedElement> rankedList = new ArrayList<RankedElement>();
	
	private List<String> datasetsToRank;
	private Map<String, Float> weightFilter;
	private RankBy level;
	

	public UserDrivenRanking(List<String> datasets, Map<String, Float> weightFilter, RankBy level){
		this.datasetsToRank = datasets;
		this.weightFilter = weightFilter;
		this.level = level;
		
		this.rank();
	}
	
	private void rank(){		
		for(String dataset : datasetsToRank){
			Map<String,List<Observation>> observations = DAQHelper.getQualityMetadataObservations(dataset);
			float totalMetricValue = 0.0f;
			
			
			switch(level) {
				case CATEGORY	:	totalMetricValue = this.rankByCategory(observations); break;
				case DIMENSION	:	totalMetricValue = this.rankByDimension(observations); break;
				case METRIC		: 	totalMetricValue = this.rankByMetric(observations); break;
			}
			rankedList.add(new RankedElement(dataset, totalMetricValue));
		}
	}
	
	
	private float rankByMetric(Map<String,List<Observation>>observations){
		float totalMetricValue = 0.0f;
		for(String key : observations.keySet()){
			if (weightFilter.containsKey(key)){
				totalMetricValue += this.getLatestObservation(observations.get(key)).getValue() * weightFilter.get(key);
			}
		}
		return totalMetricValue;
	}
	
	private float rankByDimension(Map<String,List<Observation>>observations){
		float totalMetricValue = 0.0f;
		
		Map<String, Integer> dimCountMap = DAQHelper.getNumberOfMetricsInDimension();
		Map<String, Float> dimTotalMap = new HashMap<String, Float>();
		
		for(String key : observations.keySet()){
			Observation obs = this.getLatestObservation(observations.get(key));
			String dimension = DAQHelper.getDimensionForMetric(obs.getMetricType());
			if (weightFilter.containsKey(dimension)){
				float value = obs.getValue();
				if (dimTotalMap.containsKey(dimension)){
					value = dimTotalMap.get(dimension);
					value += obs.getValue();
				}
				dimTotalMap.put(dimension, value);
			}
		}
		
		for(String key : dimCountMap.keySet()){
			if (dimTotalMap.containsKey(key)){
				totalMetricValue += (dimTotalMap.get(key) * weightFilter.get(key)) / dimCountMap.get(key);
			}
		}
		
		return totalMetricValue;
	}
	
	private float rankByCategory(Map<String,List<Observation>>observations){
		float totalMetricValue = 0.0f;
		
		Map<String, Integer> catCountMap = DAQHelper.getNumberOfDimensionsInCategory();
		
		Map<String, Float> dimTotalMap = new HashMap<String, Float>();
		
		for(String key : observations.keySet()){
			Observation obs = this.getLatestObservation(observations.get(key));
			String dimension = DAQHelper.getDimensionForMetric(obs.getMetricType());
			String category = DAQHelper.getCategoryForDimension(ModelFactory.createDefaultModel().createResource(dimension));
			if (weightFilter.containsKey(category)){
				float value = obs.getValue();
				if (dimTotalMap.containsKey(dimension)){
					value = dimTotalMap.get(dimension);
					value += obs.getValue();
				}
				dimTotalMap.put(dimension, value);
			}
		}
		
		
		for(String key : catCountMap.keySet()){
			List<String> dimensions = DAQHelper.getDimensionsInCategory(ModelFactory.createDefaultModel().createResource(key));
			for(String dim : dimensions){
				if (dimTotalMap.containsKey(dim)){
					totalMetricValue += (dimTotalMap.get(dim) * weightFilter.get(key)) / catCountMap.get(key);
				}
			}
		}
		
		return totalMetricValue;
	}
	
	
	public List<RankedElement> getSortedList(){
		Collections.sort(rankedList); //sort ascending order
		Collections.reverse(rankedList); // reverse to have sort by descending.
		return rankedList;
	}
	
	/**
	 * Get the latest observation from a list of observations.
	 * 
	 * @param A list of Observations
	 * @return The latest observation
	 */
	private Observation getLatestObservation(List<Observation> observations){
		Collections.sort(observations);
		Collections.reverse(observations);
		return observations.get(0);
	}
}
