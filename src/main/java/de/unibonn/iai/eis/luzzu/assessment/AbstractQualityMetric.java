package de.unibonn.iai.eis.luzzu.assessment;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;
import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;

public abstract class AbstractQualityMetric implements QualityMetric{

	// This method is generic to all metrics
	public List<Statement> toDAQTriples() {
		List<Statement> lst = new ArrayList<Statement>();
		
		//Resource generatedURI = Commons.generateURI();
		Resource generatedObs = Commons.generateURI();
		
//		Statement type = new StatementImpl(generatedURI, RDF.type, this.getMetricURI().asResource());
//		Statement hasObs = new StatementImpl(generatedURI, DAQ.hasObservation, generatedObs);
		
		Statement obsType = new StatementImpl(generatedObs, RDF.type, CUBE.Observation);
		Statement dc = new StatementImpl(generatedObs, DAQ.dateComputed, Commons.generateCurrentTime());
		Statement val = new StatementImpl(generatedObs, DAQ.value, Commons.generateDoubleTypeLiteral(this.metricValue()));
		Statement met = new StatementImpl(generatedObs, DAQ.metric, this.getMetricURI().asResource());
		Statement qbDS = new StatementImpl(generatedObs, CUBE.dataSet, Commons.generateRDFBlankNode());
		Statement computedOn = new StatementImpl(generatedObs, DAQ.computedOn, Commons.generateRDFBlankNode());

		
//		lst.add(type);
//		lst.add(hasObs);
		lst.add(dc);
		lst.add(val);
		lst.add(obsType);
		lst.add(met);
		lst.add(qbDS);
		lst.add(computedOn);
		
		return lst;
	}
	
	// The following classes are implemented by the metric itself
	public abstract void compute(Quad quad);
	public abstract double metricValue();
	public abstract Resource getMetricURI();
	public abstract ProblemList<?> getQualityProblems();
}
