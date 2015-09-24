package de.unibonn.iai.eis.luzzu.web.visualise;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;

import de.unibonn.iai.eis.luzzu.operations.ranking.DatasetLoader;
import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.DatasetObject;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.MetricObject;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.ObservationObject;

public class Data {

	final static Logger logger = LoggerFactory.getLogger(Data.class);
	
	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> graphs = DatasetLoader.getInstance().getAllGraphs();
		
	
	public static String getLatestObservationForDataset(String dataset){
		String graphName = graphs.get(strippedURI(dataset));
		
		Model qualityMetadata = ModelFactory.createDefaultModel();
		qualityMetadata.add(d.getNamedModel(graphName));
		qualityMetadata.add(InternalModelConf.getFlatModel());
	
		String selectQuery = "";
		
		URL url = Resources.getResource("DatasetCDM.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), qualityMetadata);
		
		Set<MetricObject> mos = new HashSet<MetricObject>();
		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution sol = set.next();
			MetricObject mo = new MetricObject();
	
			String metricName = sol.get("metric_name").asLiteral().toString();
			String dimensionName = sol.get("dimension_name").asLiteral().toString();
			String categoryName = sol.get("category_name").asLiteral().toString();
			Resource metric = sol.get("metric").asResource();

			mo.setInCategory(categoryName);
			mo.setInDimension(dimensionName);
			mo.setName(metricName);
			mo.setUri(metric.getURI());
			mo.setLatestValue(getLatestObservation(extractObservations(d.getNamedModel(graphName),metric)).getValue() * 100.0);
			
			mos.add(mo);
		}
		
		
		String json = "{ \"metrics\" : [";
		for(MetricObject mo : mos){
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			try {
				json += ow.writeValueAsString(mo) + ",";
			} catch (JsonProcessingException e) {
				logger.error("Error transforming to json : {}", e.getMessage());
			}
		}
		json = json.substring(0, json.length()-1);
		json += "]}";
		
		return json;
		
	}
	
	public static String getLatestValueForMetrics(String metric, List<String> chosenDataset){
		
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		
		URL url = Resources.getResource("GetMetricObservationValue.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		Set<DatasetObject> dsObjects = new HashSet<DatasetObject>();
		
		for (String ds : chosenDataset){
			String sel = selectQuery;
			sel = sel.replace("%metric%", "<"+metric+">");
			sel = sel.replace("%dataset%", "<"+ds.replace("\"", "")+">");
			
			QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(sel), getFlatModel());
			
			ResultSet set = exec.execSelect();
			while(set.hasNext()){
				QuerySolution sol = set.next();
				String metricName = sol.get("metric_name").asLiteral().toString();
				double value = sol.get("value").asLiteral().getDouble();
				
				DatasetObject dso = new DatasetObject();
				dso.setName(ds);
				
				MetricObject mo = new MetricObject();
				mo.setName(metricName);
				mo.setUri(metric);
				mo.setLatestValue(value * 100.0);
				
				dso.getMetrics().add(mo);
				
				dsObjects.add(dso);
			}
		}
		
		String json = "{ \"datasets\" : [";
		for(DatasetObject dso : dsObjects){
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			try {
				json += ow.writeValueAsString(dso) + ",";
			} catch (JsonProcessingException e) {
				logger.error("Error transforming to json : {}", e.getMessage());
			}
		}
		json = json.substring(0, json.length()-1);
		json += "]}";
		
		return json;
	}
	
	private static Model getFlatModel() {
		Model m = ModelFactory.createDefaultModel();
		
		Iterator<String> iter = d.listNames();
		while (iter.hasNext()){
			m.add(d.getNamedModel(iter.next()));
		}
		m.add(d.getDefaultModel());
		
		return m;
	}

	private static String strippedURI(String dataset){
		String stripped = dataset.replace("http://", "");
		if (stripped.charAt(stripped.length() - 1) == '/'){
			stripped = stripped.substring(0,stripped.length() - 1);
		}
		return stripped;
	}
	public static String getObservationsForDataset(String dataset, List<String> chosenMetrics){
		String graphName = graphs.get(strippedURI(dataset));
		
		Model qualityMetadata = ModelFactory.createDefaultModel();
		qualityMetadata.add(d.getNamedModel(graphName));
		qualityMetadata.add(InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		
		URL url = Resources.getResource("DatasetCDM.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), qualityMetadata);
		
		DatasetObject  dso = new DatasetObject();
		dso.setName(dataset);
		
		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution sol = set.next();
			MetricObject mo = new MetricObject();
			Resource metric_uri = sol.get("metric_uri").asResource();
			
			if (chosenMetrics.contains("\""+metric_uri.getURI()+"\"")){
				String metricName = sol.get("metric_name").asLiteral().toString();
				String dimensionName = sol.get("dimension_name").asLiteral().toString();
				String categoryName = sol.get("category_name").asLiteral().toString();
				Resource metric = sol.get("metric").asResource();
	
				mo.setInCategory(categoryName);
				mo.setInDimension(dimensionName);
				mo.setName(metricName);
				mo.setUri(metric_uri.getURI());
				
				List<Observation> lst_obs = extractObservations(d.getNamedModel(graphName),metric);
				Collections.sort(lst_obs);
				for(Observation obs : lst_obs){
					ObservationObject obso = new ObservationObject();
					obso.setObservationDate(obs.getDateComputed());
					obso.setObservationValue(obs.getValue()* 100.0);
					mo.getLstObservations().add(obso);
				}
				mo.setLatestValue(getLatestObservation(lst_obs).getValue() );
				
				dso.getMetrics().add(mo);
			}
		}
		
		
		String json ="";
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		try {
			json = ow.writeValueAsString(dso);
		} catch (JsonProcessingException e) {
			logger.error("Error transforming to json : {}", e.getMessage());
		}
		
		return json;
	}
	
	
	private static List<Observation> extractObservations(Model qualityMD, Resource metric){
		List<Observation> lst = new ArrayList<Observation>(); 
		
		NodeIterator iter = qualityMD.listObjectsOfProperty(metric, DAQ.hasObservation);
		while(iter.hasNext()){
			Resource res = iter.next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, qualityMD.createProperty("http://purl.org/linked-data/sdmx/2009/dimension#timePeriod")).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//get value
			Double value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getDouble();
			
			Observation obs = new Observation(res, date, value, null);
			lst.add(obs);
		}
		
		return lst;
	}
	
	private static Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		return sdf.parse(date);
	}
	
	private static Observation getLatestObservation(List<Observation> observations){
		Collections.sort(observations);
		Collections.reverse(observations);
		return observations.get(0);
	}
}
