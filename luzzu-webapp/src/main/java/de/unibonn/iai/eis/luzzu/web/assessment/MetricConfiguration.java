package de.unibonn.iai.eis.luzzu.web.assessment;

import java.io.File;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

/**
 * @author Jeremy Debattista
 *
 * This class deals with metric setting vis-a-vie
 * the Web UI communication e.g. getting a list 
 * of all installed metrics
 * 
 */
public class MetricConfiguration {

	public static Model getAllMetrics(){
		File externalsFolder = new File("externals/metrics/");
		File[] listOfFiles = externalsFolder.listFiles();
		
		Model returnModel = ModelFactory.createDefaultModel();
		
		for(File metrics : listOfFiles){
			if (metrics.isHidden()) continue;
			if (!metrics.isDirectory()) continue;
			
			//If we have a POM file then we should load dependencies
			Model m = ModelFactory.createDefaultModel();
			m.read(metrics+"/metrics.trig");
			
			ResIterator res = m.listSubjectsWithProperty(RDF.type, LMI.LuzzuMetricJavaImplementation);
			while (res.hasNext()){
				Resource r = res.next();
				String jpn = m.listObjectsOfProperty(r, LMI.javaPackageName).next().asLiteral().toString();
				
				NodeIterator n = m.listObjectsOfProperty(r, RDFS.label);
				String label = "";
				if (n.hasNext()) label = n.next().asLiteral().toString();
				
				//TODO: lmi:luzzuMetricJavaImpl linked with daq:Metric lmi:referTo. Possibly extracting labels and comments 
				// from the metric ontology found in the InternalModel (InternalConfigModel.getFlatModel()).
				// group in category,dimension
				
				Resource bn = Commons.generateRDFBlankNode().asResource();
				returnModel.add(bn, LMI.javaPackageName, returnModel.createLiteral(jpn));
				returnModel.add(bn, RDFS.label, returnModel.createLiteral(label));
				
			}
		}
		
		return returnModel;
	}
	
	
	
	//TODO: allow setting of before and after
	
}
