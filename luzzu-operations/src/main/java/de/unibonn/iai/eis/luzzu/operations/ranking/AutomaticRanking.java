package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.operations.datatypes.RankedElement;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 *
 * Luzzu enables a fair and unbiased policy in ranking datasets.
 * With the Automatic Ranking algorithm, the idea is to promote
 * (a) High Quality Datasets;
 * (b) Datasets on which a larger number of quality metrics is calculated.
 * Therefore, datasets of poor quality but having more quality metadata 
 * might end up ranked higher than those with excellent quality on 
 * the only metric assessed on.
 *
 */
@Deprecated
public class AutomaticRanking {

	private double thetaWeight = 0.0d;
	private double rhoWeight = 0.0d;
	
	/**
	 * Ranks a list of datasets based on the chosen filters
	 * 
	 * @param List of dataset URIs
	 * @param Set of chosen filters (e.g. urn:RDFAccessibilityMetric)
	 * @return A list of Ranked Datasets (RankedElement).
	 */
	public List<RankedElement> rank(List<String> datasets, Set<String> filtersChosen){
		return this.rank(datasets, filtersChosen, "", false);
	}
	
	public List<RankedElement> rank(List<String> datasets, String categoryOrDimensionUri, boolean isCategory){
		return this.rank(datasets, new HashSet<String>() ,categoryOrDimensionUri, isCategory);
	}
	
	private List<RankedElement> rank(List<String> datasets, Set<String> filtersChosen, String categoryOrDimensionUri, boolean isCategory){
		List<RankedElement> sortedElements = new ArrayList<RankedElement>();
		
		int totMet = 0;
		if (categoryOrDimensionUri.equals(""))	totMet = this.getTotalNumberOfMetrics(datasets);
		else {
			if (isCategory) totMet = this.getTotalNumberOfMetricsForCategory(datasets, categoryOrDimensionUri);
			else totMet = this.getTotalNumberOfMetricsForDimension(datasets, categoryOrDimensionUri);
		}
		this.setWeights(totMet, filtersChosen.size());
		
		for(String ds : datasets){
			float tau = 0.0f;
			float thetaTotal = 0.0f;
			float rhoTotal = 0.0f;
			
			//loop results
			Map<String,List<Observation>> observations  = this.extractObservations(ds);
			for(String metricType : observations.keySet()){
				Observation obs = this.getLatestObservation(observations.get(metricType));
				
				
				if (!(categoryOrDimensionUri.equals(""))){
					//ranking at category or dimension level
					if (isCategory){
						if (DAQHelper.getCategoryResource(ModelFactory.createDefaultModel().createResource(metricType)).getURI().equals(categoryOrDimensionUri))
						{
							double val = this.rhoWeight * obs.getValue();
							rhoTotal += val;
						}
					} else {
						if (DAQHelper.getDimensionResource(ModelFactory.createDefaultModel().createResource(metricType)).getURI().equals(categoryOrDimensionUri))
						{
							double val = this.rhoWeight * obs.getValue();
							rhoTotal += val;
						}
					}
				} else {
					if (filtersChosen.contains(metricType)){
						double val = this.thetaWeight * obs.getValue();
						thetaTotal += val;
					} else {
						double val = this.rhoWeight * obs.getValue();
						rhoTotal += val;
					}
				}
			}
			
			tau = thetaTotal + rhoTotal;
			sortedElements.add(new RankedElement(ds, tau));
		}
		
		Collections.sort(sortedElements); //sort ascending order
		Collections.reverse(sortedElements); // reverse to have sort by descending
		
		return sortedElements;
	}

	
	/**
	 * Calculate the maximum number of metrics used in the set of datasets
	 * 
	 * @param List of datasets to be ranked
	 * @return The maximum number of metrics
	 */
	private int getTotalNumberOfMetrics(List<String> datasets){
		int metrics = 0;
		for(String ds : datasets){
			Dataset d = RDFDataMgr.loadDataset(ds);

			Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
			Model qualityMD = d.getNamedModel(graph.getURI());
			
			int _retMetNo = DAQHelper.getNumberOfMetricsInDataSet(qualityMD);
			metrics = (metrics <= _retMetNo) ? _retMetNo : metrics;
		}
		
		return metrics;
	}
	
	private int getTotalNumberOfMetricsForDimension(List<String> datasets, String dimensionUri){
		int metrics = 0;
		for(String ds : datasets){
			Dataset d = RDFDataMgr.loadDataset(ds);

			Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
			Model qualityMD = d.getNamedModel(graph.getURI());
			
			int _retMetNo = DAQHelper.getNumberOfMetricsInDataSet(qualityMD, qualityMD.createResource(dimensionUri), false);
			metrics = (metrics <= _retMetNo) ? _retMetNo : metrics;
		}
		
		return metrics;
	}
	
	private int getTotalNumberOfMetricsForCategory(List<String> datasets, String categoryUri){
		int metrics = 0;
		for(String ds : datasets){
			Dataset d = RDFDataMgr.loadDataset(ds);

			Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
			Model qualityMD = d.getNamedModel(graph.getURI());
			
			int _retMetNo = DAQHelper.getNumberOfMetricsInDataSet(qualityMD, qualityMD.createResource(categoryUri), true);
			metrics = (metrics <= _retMetNo) ? _retMetNo : metrics;
		}
		
		return metrics;
	}
	
	/**
	 * Extract all observations from a dataset
	 * @param dataset URI
	 * @return a HashMap with the metric type being the key and a list of observations
	 */
	private Map<String,List<Observation>> extractObservations(String dataset){
		Dataset d = RDFDataMgr.loadDataset(dataset);
		Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
		Model qualityMD = d.getNamedModel(graph.getURI());
		
		Map<String,List<Observation>> map = new HashMap<String,List<Observation>>(); // metric resource, list<observations>
		
		ResIterator iter = qualityMD.listResourcesWithProperty(RDF.type, CUBE.Observation);
		while(iter.hasNext()){
			Resource res = iter.next();
			
			//get metric uri
			Resource metricURI = qualityMD.listObjectsOfProperty(res, DAQ.metric).next().asResource();
			//get metric type
			String metricType = qualityMD.listObjectsOfProperty(metricURI, RDF.type).next().asResource().toString();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, DC.date).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//get value
			Float value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getFloat();
			
			Observation obs = new Observation(res, date, value, null);
			
			if (!(map.containsKey(metricType))){
				map.put(metricType, new ArrayList<Observation>());
			}
			map.get(metricType).add(obs);
		}
		
		return map;
	}
	
	/**
	 * Sets theta and rho, which will be used to rank datasets
	 * 
	 * @param totNumberMetrics
	 * @param totFilterChosen
	 */
	private void setWeights(int totNumberMetrics, int totFilterChosen){
		this.thetaWeight = (double) (1/((double)totFilterChosen + 1));
		this.rhoWeight = (double) (1 / (((double)totFilterChosen + 1) * ((double)totNumberMetrics - (double)totFilterChosen)));
	}
	
	/**
	 * Formats an xsd:dateTime to a JAVA object data
	 * @param date - A string extracted from the triple's object
	 * @return JAVA object data
	 * 
	 * @throws ParseException
	 */
	private Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		return sdf.parse(date);
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
