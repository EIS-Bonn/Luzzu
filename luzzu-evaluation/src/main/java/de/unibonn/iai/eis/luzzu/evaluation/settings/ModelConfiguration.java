package de.unibonn.iai.eis.luzzu.evaluation.settings;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

public class ModelConfiguration {
	
	public static Model getModelConfiguration(int initialisedMetrics){
		List<String> metricList = new ArrayList<String>();
		switch (initialisedMetrics){
			case 0 : break;
			default : break;
		}
		
		Model m = ModelFactory.createDefaultModel();
		
		for (String metric : metricList){
			m.createStatement(Commons.generateURI(), LMI.metric, metric);
		}
		
		return m;
	}	
	
}
