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
			case 9 : metricList.add("eu.diachron.qualitymetrics.intrinsic.conciseness.ExtensionalConcisenessBloom");
			case 8 : metricList.add("eu.diachron.qualitymetrics.intrinsic.accuracy.MalformedDatatypeLiterals");
			case 7 : metricList.add("eu.diachron.qualitymetrics.representational.conciseness.ShortURIs");
			case 6 : metricList.add("eu.diachron.qualitymetrics.representational.understandability.HumanReadableLabelling");
			case 5 : metricList.add("eu.diachron.qualitymetrics.accessibility.interlinking.LinkExternalDataProviders");
			case 4 : metricList.add("eu.diachron.qualitymetrics.accessibility.availability.DereferencibilityBackLinks");
			case 3 : metricList.add("eu.diachron.qualitymetrics.accessibility.licensing.MachineReadableLicense");
			case 2	: metricList.add("eu.diachron.qualitymetrics.accessibility.licensing.HumanReadableLicense");
			case 1 : metricList.add("eu.diachron.qualitymetrics.accessibility.availability.DereferencibilityForwardLinks");break;
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
