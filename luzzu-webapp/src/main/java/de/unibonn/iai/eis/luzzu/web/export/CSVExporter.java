package de.unibonn.iai.eis.luzzu.web.export;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
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
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.luzzu.operations.ranking.DatasetLoader;
import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;

public class CSVExporter {
	
	final static Logger logger = LoggerFactory.getLogger(CSVExporter.class);


	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> datasets_metadata = DatasetLoader.getInstance().getAllGraphs();
	
	public static StringBuilder exportAllDatasets(){
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());

		String selectQuery = "";
		
		List<MetricDescription>  mdset = getMetricsUsed();
		
		StringBuilder csvFile = new StringBuilder();
		csvFile.append(",");
		for(MetricDescription md : mdset){
			csvFile.append(md.name);
			csvFile.append(",");
		}
		csvFile.deleteCharAt(csvFile.lastIndexOf(","));
		csvFile.append(System.lineSeparator());
		System.out.println();
		URL url = Resources.getResource("GetLatestObservedValuesForDataset.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		
		for (String ds : datasets_metadata.keySet()){
			csvFile.append(ds); 
			csvFile.append(",");
	
			QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), d.getNamedModel(datasets_metadata.get(ds)));

			ResultSet set = exec.execSelect();
			while(set.hasNext()){
				QuerySolution qs = set.next();
				double value = qs.get("value").asLiteral().getDouble();
				csvFile.append(value);
				csvFile.append(",");
			}

			csvFile.deleteCharAt(csvFile.lastIndexOf(","));
			csvFile.append(System.lineSeparator());
		}

		return csvFile;
	}
	
	
	private static List<MetricDescription> getMetricsUsed(){
		String selectQuery = "";
		
		URL url = Resources.getResource("facets.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), getFlatModel());
		ResultSet set = exec.execSelect();
		
		List<MetricDescription> retSet = new ArrayList<MetricDescription>();
		while(set.hasNext()){
			QuerySolution qs = set.next();
			String metURI = qs.get("metric").asResource().toString();
			String met = qs.get("metric_name").asLiteral().toString();
			
			MetricDescription md = new MetricDescription();
			md.uri = metURI;
			md.name = met;
			retSet.add(md);
		}
		
		return retSet;
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
	
	private static class MetricDescription{
		protected String name;
		protected String uri;
		
		public boolean equals(Object obj){
			return ((MetricDescription)obj).uri.equals(this.uri);
		}
		
		public int hashCode(){
			return this.uri.hashCode();
		}
	}
	
}
