package de.unibonn.iai.eis.luzzu.web.visualise;


import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.luzzu.operations.ranking.DatasetLoader;
import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.utilities.SPARQLHelper;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.DatasetObject;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.MetricObject;

public class Wizard {
	
	final static Logger logger = LoggerFactory.getLogger(Wizard.class);
	
	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	
	public static String getAllDatasets(){
		String selectQuery = "SELECT DISTINCT ?dataset { ?x " + SPARQLHelper.toSPARQL(DAQ.computedOn) +" ?dataset . }";
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), getFlatModel());
		
		String json = "[";
		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution sol = set.next();
			json += "\""+sol.get("dataset").asResource().toString() + "\""+",";
		}
		json = json.substring(0, json.length()-1);
		json += "]";
		
		return json;
	}
	
	public static String getAllCommonDatasetsMetrics(){
		
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());

		String selectQuery = "";
		
		URL url = Resources.getResource("DatasetMetrics.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), getFlatModel());
		Map<String, DatasetObject> datasetObjects = new HashMap<String, DatasetObject>();
		Map<String, MetricObject> metricObjects = new HashMap<String, MetricObject>();


		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution sol = set.next();
			String ds = sol.get("dataset").asResource().toString();
			String metricName = sol.get("metric_name").asLiteral().toString();
			String metric = sol.get("metric").asResource().toString();
			
			MetricObject mo = null;
			DatasetObject dso = null;
			if (datasetObjects.containsKey(ds)){
				dso = datasetObjects.get(ds);
				
				if(metricObjects.containsKey(metric)){
					if (!(dso.getMetrics().contains(metric))){
						mo = metricObjects.get(metric);
						mo.getCommonDatasets().add(ds);
						dso.getMetrics().add(mo);
					}
				} else {
					mo = new MetricObject();
					mo.setName(metricName);
					mo.setUri(metric);
					mo.getCommonDatasets().add(ds);
					dso.getMetrics().add(mo);
					metricObjects.put(metric, mo);
				}
			} else {
				dso = new DatasetObject();
				dso.setName(ds);
				
				if(metricObjects.containsKey(metric)){
					if (!(dso.getMetrics().contains(metric))){
						mo = metricObjects.get(metric);
						mo.getCommonDatasets().add(ds);
						dso.getMetrics().add(mo);
					}
				} else {
					mo = new MetricObject();
					mo.setName(metricName);
					mo.setUri(metric);
					mo.getCommonDatasets().add(ds);
					dso.getMetrics().add(mo);
					metricObjects.put(metric, mo);
				}
				datasetObjects.put(ds, dso);
			}
		}
		
		
		String json = "{ \"metrics\" : [";
		for(MetricObject mo : metricObjects.values()){
			if (mo.getCommonDatasets().size() <= 1) continue;
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
	
	public static String getAllDatasetsWithMultipleObservations(){
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		//getFlatModel().write(new FileOutputStream(new File("/Users/jeremy/Desktop/model.ttl")), "TURTLE");
		URL url = Resources.getResource("DatasetsMultObs.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		Model m = getFlatModel();
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery, Syntax.syntaxARQ), m);
		
		Map<String, DatasetObject> dsmap = new HashMap<String, DatasetObject>();
		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution sol = set.next();
			String dataset = sol.get("dataset").asResource().getURI();
			String metric_name = sol.get("metric_name").asLiteral().toString();
			String metric_uri = sol.get("metric").asResource().getURI();
			
			MetricObject mo = new MetricObject();
			mo.setName(metric_name);
			mo.setUri(metric_uri);
			if (dsmap.containsKey(dataset)) dsmap.get(dataset).getMetrics().add(mo);
			else {
				DatasetObject dso = new DatasetObject();
				dso.setName(dataset);
				dso.getMetrics().add(mo);
				dsmap.put(dataset, dso);
			}
			
		}
		
		String json = "{ \"datasets\" : [";
		for(DatasetObject dso : dsmap.values()){
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


}
