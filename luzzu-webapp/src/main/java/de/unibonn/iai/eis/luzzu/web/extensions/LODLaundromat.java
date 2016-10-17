package de.unibonn.iai.eis.luzzu.web.extensions;

import java.io.IOException;
import java.math.RoundingMode;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
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

import de.unibonn.iai.eis.luzzu.operations.ranking.DatasetLoader;
import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.web.visualise.Data;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.DatasetObject;
import de.unibonn.iai.eis.luzzu.web.visualise.datatypes.MetricObject;

public class LODLaundromat {
	final static Logger logger = LoggerFactory.getLogger(Data.class);
	
	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> graphs = DatasetLoader.getInstance().getAllGraphs();

	public static String getQualityStats(){
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		
		URL url = Resources.getResource("GetMetricObservationValue.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		String[] metrics = new String[]{"<http://www.diachron-fp7.eu/dqm#CorrectLanguageTag>","<http://www.diachron-fp7.eu/dqm#CompatibleDatatype>"};
		String[] jsonString = new String[]{"averageCorrectLanguageTag","averageCompatibleDatatype"};
		
		double sum = 0.0;
		double counter = 0.0;
		String json = "{";
		
		json += "\"totalAssessedFiles\" : " + graphs.size() + ",";
		int c = 0;
		
		for (String m : metrics){
			System.out.println(jsonString[c]);
			for (String ds : graphs.keySet()){
				String sel = selectQuery;
				sel = sel.replace("%metric%", m);
				sel = sel.replace("%dataset%", "<http://"+ds+">");
				
				QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(sel), getFlatModel());
				
				ResultSet set = exec.execSelect();
				while(set.hasNext()){
					QuerySolution sol = set.next();
					double val = sol.get("value").asLiteral().getDouble() * 100.0;
					
					if (!Double.isNaN(val)){
						sum += val;
						counter++;
					} 
					
					if (val < 0.4){
						System.out.println(ds);
					}
				}
			}
			double avg = (sum/counter);
			
			DecimalFormat df = new DecimalFormat("#.###");
			df.setRoundingMode(RoundingMode.CEILING);
			json += "\""+jsonString[c]+"\" : " + df.format(avg) + ",";
			c++;
		}
		
		json = json.substring(0, json.length()-1);
		json += "}";
		
		return json;
	}

	public static String getSampleForLodLaundromat(String metric){
		
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		
		URL url = Resources.getResource("GetMetricObservationValue.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		// Randomly Choosing 30;
		// TODO: reservoir sampling
		int sample_size = 30;
		Set<Integer> randomNumbers = new HashSet<Integer>();
		while (randomNumbers.size() <= sample_size){
			Random r = new Random();
			int  n = r.nextInt(graphs.size());
			randomNumbers.add(n);
		}
		
		Set<DatasetObject> dsObjects = new HashSet<DatasetObject>();
		
		int counter = 0;
		for (String ds : graphs.keySet()){
			if (!(randomNumbers.contains(counter))) {
				counter++;
				continue;
			}
			counter++;
			
			
			String sel = selectQuery;
			sel = sel.replace("%metric%", "<"+metric+">");
			sel = sel.replace("%dataset%", "<http://"+ds+">");
			
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
				
				if (!Double.isNaN(value)){
					dsObjects.add(dso);
				} else {
					randomNumbers.add(counter + 1);
					continue;
				}
				
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

}
