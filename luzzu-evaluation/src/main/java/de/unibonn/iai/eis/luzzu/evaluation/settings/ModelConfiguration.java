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
			case 13 : metricList.add("eu.diachron.qualitymetrics.intrinsic.consistency.HomogeneousDatatypes"); 
			case 12 : metricList.add("eu.diachron.qualitymetrics.accessibility.interlinking.InterlinkDetectionMetric");
			case 11 : metricList.add("eu.diachron.qualitymetrics.accessibility.licensing.MachineReadableLicense");
			case 10 : metricList.add("eu.diachron.qualitymetrics.accessibility.interlinking.LinkExternalDataProviders");
			case 9 : metricList.add("eu.diachron.qualitymetrics.accessibility.availability.DereferencibilityForwardLinks");
			case 8 : metricList.add("eu.diachron.qualitymetrics.accessibility.availability.DereferencibilityBackLinks");
			case 7 : metricList.add("eu.diachron.qualitymetrics.accessibility.licensing.HumanReadableLicense");
			case 6 : metricList.add("eu.diachron.qualitymetrics.representational.understandability.HumanReadableLabelling");
			case 5 : metricList.add("eu.diachron.qualitymetrics.representational.conciseness.ShortURIs");
			case 4 : metricList.add("eu.diachron.qualitymetrics.intrinsic.accuracy.MalformedDatatypeLiterals");
			case 3 : metricList.add("eu.diachron.qualitymetrics.intrinsic.conciseness.ExtensionalConciseness");
			case 2 : metricList.add("eu.diachron.qualitymetrics.intrinsic.conciseness.DuplicateInstance");
			case 1 : metricList.add("eu.diachron.qualitymetrics.contextual.amountofdata.AmountOfTriples"); break;
			case 0 : break;
			default : break;
		}
		
		Model m = ModelFactory.createDefaultModel();
		
		for (String metric : metricList){
			m.add(m.createStatement(Commons.generateURI(), LMI.metric, metric));
		}
		
		return m;
	}	
	
}
