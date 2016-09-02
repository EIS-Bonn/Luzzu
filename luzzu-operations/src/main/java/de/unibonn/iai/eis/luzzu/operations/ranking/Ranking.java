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
import com.hp.hpl.jena.query.QuerySolution;
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

	private int isNaNMetrics = 0;
	private int isNaNDimension = 0;

	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig){
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
		double rankingValue = 0.0d;
		logger.info("Ranking by Metric {}, with the weight of {}",metric.getURI(), weight);

		List<Observation> lst = ObservationHelper.extractObservations(currentQualityMetadata, metric);
		if (lst.size() > 0){
			Observation obs = ObservationHelper.getLatestObservation(lst);
			rankingValue = obs.getValue() * weight;
			currentComputedOn = obs.getComputedOn().getURI();
			currentGraphURI = obs.getGraphURI().getURI();
		} else {
			String graph = DatasetLoader.getInstance().getAllGraphs().get(currentComputedOn);
			currentGraphURI = graph;
			currentComputedOn = (getComputedOn() != null) ? getComputedOn() : currentComputedOn;
		}
	
		if (((Double)rankingValue).isNaN()){
			this.isNaNMetrics++;
			return 0.0d;
		}

		return rankingValue;
	}
	
	private double dimensionValue(Resource dimension, double weight){
		logger.info("Ranking by Dimension {}, with the weight of {}",dimension.getURI(), weight);
		String selectQuery = this.getSelectQuery("sparql/GetDimensionMetrics.sparql").replace("%dimension%", SPARQLHelper.toSPARQL(dimension));
		
		double totalNumberOfMetrics = 0.0;
		double summation = 0.0;
		this.isNaNMetrics = 0;
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		while(set.hasNext()){
			totalNumberOfMetrics++;
			Resource res = set.next().get("metric").asResource();

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
			double dimensionRanking = summation / (totalNumberOfMetrics - this.isNaNMetrics);
			logger.info("Ranking value for {} computed: {}",dimension.getURI(),dimensionRanking);
			
			if (((Double)dimensionRanking).isNaN()){
				this.isNaNDimension++;
				return 0.0d;
			}
			return dimensionRanking;
		}
	}
	
	private double categoryValue(Resource category, double weight){
		logger.info("Ranking by Category {}, with the weight of {}",category.getURI(), weight);
		String selectQuery = this.getSelectQuery("sparql/GetCategoryDimensions.sparql").replace("%category%", SPARQLHelper.toSPARQL(category));
		
		double totalNumberOfDimensions = 0.0;
		double summation = 0.0;
		this.isNaNDimension = 0;
		
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
			double categoryRanking = summation / (totalNumberOfDimensions - this.isNaNDimension);
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
		List<RelativePosition> relativeRanks = this.setRelativePosition(ranks);
		
		RelativePosition tmp = new RelativePosition(baseToURI.get(currentQMDBase),-1);
		int bottomPos = relativeRanks.get(relativeRanks.size() - 1).getPos();
		double val = 0.0d;
		
		
		if (relativeRanks.get(relativeRanks.indexOf(tmp)).getPos() == 1) val = 100.0d;
		else if (relativeRanks.get(relativeRanks.indexOf(tmp)).getPos() == bottomPos) val = 0.0d;
		else {
			int thePos = relativeRanks.get(relativeRanks.indexOf(tmp)).getPos();
			val = ((((double)ranks.size() + 1) - (double)thePos) * 100.0) / (double) ranks.size();
		}
		return (val/100) * weight; // divide by 100 to get back to a number between 0 and 1
		
//		RankedObject obj = new RankedObject(baseToURI.get(currentQMDBase), 0.0, currentGraphURI);
//		double val = 0.0d;
//		
//		if (ranks.indexOf(obj) == 0) val = 100.0d;
//		else if (ranks.indexOf(obj) == ranks.size() - 1) val = 0.0d;
//		else val = (((double)ranks.size() - ((double)ranks.indexOf(obj))) * 100.0) / ((double) ranks.size());
//		return val * weight;
	}
	
	public String getComputedOn(){
		String selectQuery = "SELECT ?cOn { graph <"+currentGraphURI+"> { ?s <"+DAQ.computedOn.getURI()+"> ?cOn } }";
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), DatasetLoader.getInstance().getInternalDataset());
		
		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution qs = set.next();
			Resource computedOn = qs.getResource("cOn");
			return computedOn.getURI();
		}
		return null;
	}
	
	
	private List<RelativePosition> setRelativePosition(List<RankedObject> obj){
		List<RelativePosition> lst = new ArrayList<RelativePosition>();
		
		int rank = 0;
		double lastValue = -1.0;
		
		for(RankedObject o : obj){
			double val = o.getRankedValue();
			if (lastValue != val){
				rank = obj.indexOf(o) + 1;
				lastValue = o.getRankedValue();
			} 
			RelativePosition _rp = new RelativePosition(o.getDataset(), rank);
			lst.add(_rp);
		}
		
		return lst;
	}
	
	public List<RankedObject> rankIntegerBasedMetric(Resource metric){
		List<RankedObject> rankedObjects = new ArrayList<RankedObject>();
		
		Map<String,String> graphs = DatasetLoader.getInstance().getAllGraphs();
		
		for(String base : graphs.keySet()){
			String graph = graphs.get(base);
			currentQMDBase = base;
			currentQualityMetadata = d.getNamedModel(graph);
			currentComputedOn = base;
			
			Double rankedValue = metricIntegerToPercentage(metric, 1.0d);
			
			if (!(rankedValue.isNaN())){
				RankedObject ro = new RankedObject(baseToURI.get(currentComputedOn), rankedValue, currentGraphURI);
				rankedObjects.add(ro);
			}
			
			//baseToURI.put(currentQMDBase, currentComputedOn);
		}
		Collections.sort(rankedObjects);
		Collections.reverse(rankedObjects);
		return rankedObjects;
	}

	private class RelativePosition{
		private int pos = 0;
		private String dataset = "";
		
		public RelativePosition(String dataset, int pos){
			this.pos = pos;
			this.dataset = dataset;
		}
		
		public int getPos(){
			return pos;
		}
		
		@Override
		public boolean equals(Object other){
			if (other instanceof RelativePosition){
				RelativePosition _other = (RelativePosition) other;
				return this.dataset.equals(_other.dataset);
			} else return false;
		}
		
		@Override
		public int hashCode(){
			return this.dataset.hashCode();
		}
	}

}
