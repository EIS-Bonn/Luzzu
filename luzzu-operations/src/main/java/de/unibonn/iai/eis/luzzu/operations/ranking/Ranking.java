package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.utilities.ObservationHelper;
import de.unibonn.iai.eis.luzzu.semantics.utilities.SPARQLHelper;

public class Ranking {

	final static Logger logger = LoggerFactory.getLogger(Ranking.class);

	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	
	private Model currentQualityMetadata;

	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig ){
		List<RankedObject> rankedObjects = new ArrayList<RankedObject>();
		
		Map<String,String> graphs = DatasetLoader.getInstance().getAllGraphs();
		
		for(RankingConfiguration rc : rankingConfig){
			for(String base : graphs.keySet()){
				String graph = graphs.get(base);
				currentQualityMetadata = d.getNamedModel(graph);
				
				double rankedValue = -1.0;
				
				if (rc.getType() == RankBy.CATEGORY){
					rankedValue = this.categoryValue(rc.getUriResource(), rc.getWeight());
				}
				if (rc.getType() == RankBy.DIMENSION){
					rankedValue = this.dimensionValue(rc.getUriResource(), rc.getWeight());
				}
				if (rc.getType() == RankBy.METRIC){
					rankedValue = this.metricValue(rc.getUriResource(), rc.getWeight());
				}
				
				if (rankedValue > -1.0){
					RankedObject ro = new RankedObject(base, rankedValue);
					rankedObjects.add(ro);
				} // else dataset is not assessed with the chosen filter
			}
		}
		
		Collections.sort(rankedObjects);
		return rankedObjects;
	}
	
	private double metricValue(Resource metric, double weight){
		double rankingValue = -1.0;
		
		logger.info("Ranking by Metric {}, with the weight of {}",metric.getURI(), weight);

		List<Observation> lst = ObservationHelper.extractObservations(currentQualityMetadata, metric);
		if (lst != null){
			Observation obs = ObservationHelper.getLatestObservation(lst);
			rankingValue = obs.getValue() * weight;
		}
		
		return rankingValue;
	}
	
	private double dimensionValue(Resource dimension, double weight){
		logger.info("Ranking by Dimension {}, with the weight of {}",dimension.getURI(), weight);
		String selectQuery = this.getSelectQuery("GetDimensionMetrics.sparql").replace("%dimension%", SPARQLHelper.toSPARQL(dimension));
		
		double totalNumberOfMetrics = 0.0;
		double summation = 0.0;
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		while(set.hasNext()){
			totalNumberOfMetrics++;
			Resource res = set.next().get("metric").asResource();
			summation += this.metricValue(res,weight);
		}
		
		if (totalNumberOfMetrics == 0.0){
			logger.info("No dimensions available for the {} Dimension",dimension.getURI());
			return -1.0;
		} else{
			double dimensionRanking = summation / totalNumberOfMetrics;
			logger.info("Ranking value for {} computed: {}",dimension.getURI(),dimensionRanking);
			return dimensionRanking;
		}
	}
	
	private double categoryValue(Resource category, double weight){
		logger.info("Ranking by Category {}, with the weight of {}",category.getURI(), weight);
		String selectQuery = this.getSelectQuery("GetCategoryDimensions.sparql").replace("%category%", SPARQLHelper.toSPARQL(category));
		
		double totalNumberOfDimensions = 0.0;
		double summation = 0.0;
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		while(set.hasNext()){
			totalNumberOfDimensions++;
			Resource res = set.next().get("dimension").asResource();
			summation += this.dimensionValue(res,weight);
		}
		
		if (totalNumberOfDimensions == 0.0){
			logger.info("No dimensions available for the {} Category",category.getURI());
			return -1.0;
		} else{
			double categoryRanking = summation / totalNumberOfDimensions;
			logger.info("Ranking value for {} computed: {}",category.getURI(),categoryRanking);
			return categoryRanking;
		}
	}
		
	private String getSelectQuery(String fileName){
		String selectQuery = "";
		URL url = Resources.getResource(fileName);
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		return selectQuery;
	}
}
