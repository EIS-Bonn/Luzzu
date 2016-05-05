package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.XSD;

import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.utilities.ObservationHelper;
import de.unibonn.iai.eis.luzzu.semantics.utilities.SPARQLHelper;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

public class Ranking {

	final static Logger logger = LoggerFactory.getLogger(Ranking.class);

	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static HashMap<String, String> baseToURI = new HashMap<String,String>();
	
	
	private Model currentQualityMetadata;
	private String currentQMDBase;
	private String currentComputedOn;
	private String currentGraphURI;
	
	private Map<Resource, List<RankedObject>> metricRanking = new HashMap<Resource,List<RankedObject>>();

	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig ){
		List<RankedObject> rankedObjects = new ArrayList<RankedObject>();
		
		Map<String,String> graphs = DatasetLoader.getInstance().getAllGraphs();
		
		for(String base : graphs.keySet()){
			String graph = graphs.get(base);
			currentQMDBase = base;
			currentQualityMetadata = d.getNamedModel(graph);
			currentComputedOn = base;
			
			Double rankedValue = 0.0;
			
			for(RankingConfiguration rc : rankingConfig){
				if (rc.getType() == RankBy.CATEGORY){
					rankedValue += this.categoryValue(rc.getUriResource(), rc.getWeight());
				}
				if (rc.getType() == RankBy.DIMENSION){
					rankedValue += this.dimensionValue(rc.getUriResource(), rc.getWeight());
				}
				if (rc.getType() == RankBy.METRIC){
					rankedValue += this.metricValue(rc.getUriResource(), rc.getWeight());
				}
			}
			
			if (!(rankedValue.isNaN())){
				RankedObject ro = new RankedObject(currentComputedOn, rankedValue, currentGraphURI);
				rankedObjects.add(ro);
			}
			
			baseToURI.put(currentQMDBase, currentComputedOn);
		}
		Collections.sort(rankedObjects);
		Collections.reverse(rankedObjects);
		return rankedObjects;
	}
	
	private double metricValue(Resource metric, double weight){
		double rankingValue = 0.0;
		
		logger.info("Ranking by Metric {}, with the weight of {}",metric.getURI(), weight);

		List<Observation> lst = ObservationHelper.extractObservations(currentQualityMetadata, metric);
		if (lst.size() > 0){
			Observation obs = ObservationHelper.getLatestObservation(lst);
			rankingValue = obs.getValue() * weight;
			currentComputedOn = obs.getComputedOn().getURI();
			currentGraphURI = obs.getGraphURI().getURI();
		}
		
		return rankingValue;
	}
	
	private double dimensionValue(Resource dimension, double weight){
		logger.info("Ranking by Dimension {}, with the weight of {}",dimension.getURI(), weight);
		String selectQuery = this.getSelectQuery("sparql/GetDimensionMetrics.sparql").replace("%dimension%", SPARQLHelper.toSPARQL(dimension));
		
		double totalNumberOfMetrics = 0.0;
		double summation = 0.0;
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		while(set.hasNext()){
			totalNumberOfMetrics++;
			Resource res = set.next().get("metric").asResource();
			if (res.getURI().equals("http://www.diachron-fp7.eu/dqm#ReuseExistingVocabularyMetric")) continue;

			// choose metric value type (e.g if double use the default)
			RDFNode expectedType = InternalModelConf.getFlatModel().listObjectsOfProperty(res, DAQ.expectedDataType).next();
			if (expectedType.asResource().equals(XSD.integer)){
				summation += this.metricIntegerToPercentage(res, weight);
			} else {
				// Default
				summation += this.metricValue(res,weight);	
			}
		}
		
		if (totalNumberOfMetrics == 0.0){
			logger.info("No dimensions available for the {} Dimension",dimension.getURI());
			return 0.0;
		} else{
			double dimensionRanking = summation / totalNumberOfMetrics;
			logger.info("Ranking value for {} computed: {}",dimension.getURI(),dimensionRanking);
			return dimensionRanking;
		}
	}
	
	private double categoryValue(Resource category, double weight){
		logger.info("Ranking by Category {}, with the weight of {}",category.getURI(), weight);
		String selectQuery = this.getSelectQuery("sparql/GetCategoryDimensions.sparql").replace("%category%", SPARQLHelper.toSPARQL(category));
		
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
			return 0.0;
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

	private double metricIntegerToPercentage(Resource metric, double weight){	
		if (!(metricRanking.containsKey(metric))){
			//First rank datasets on their metric value
			Ranking r = new Ranking();
			List<RankingConfiguration> rankingConfig = new ArrayList<RankingConfiguration>();
			RankingConfiguration rc = new RankingConfiguration(metric, RankBy.METRIC, 1.0);
			rankingConfig.add(rc);
			
			List<RankedObject> ranks = r.rank(rankingConfig);
			metricRanking.put(metric, ranks);
		}
		
		List<RankedObject> ranks = metricRanking.get(metric);
		RankedObject obj = new RankedObject(baseToURI.get(currentQMDBase), 0.0, currentGraphURI);
		double val = ((((double)ranks.size() + 1.0) - ((double) ranks.indexOf(obj))) * 100) / (double) ranks.size();
		return (val / 100.0) * weight;
	}
}
