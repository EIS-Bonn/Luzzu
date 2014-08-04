package de.unibonn.iai.eis.luzzu.annotations.report;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.QR;
import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;

/**
 * @author Jeremy Debattista
 * 
 * The QualityReport Class provides a number of methods
 * to enable the represention of problematic triples 
 * found during the assessment of Linked Datasets.
 * This class describes these problematic triples in
 * terms of either Reified RDF or a Sequence of Resources.
 * The Quality Report description can be found in
 * @see src/main/resource/vocabularies/qr/qr.trig
 * 
 */
public class QualityReport {
	
	/**
	 * Creates instance triples corresponding to a quality problem
	 * 
	 * @param metricURI - The metric's instance URI
	 * @param problemList - The list of problematic triples found during the assessment of the metric
	 * 
	 * @return A list of statements corresponding to a quality problem
	 */
	public List<Statement> createQualityProblem(Resource metricURI, ProblemList<?> problemList){
		List<Statement> sList = new ArrayList<Statement>();
		
		Resource problemURI = Commons.generateURI();
		
		sList.add(new StatementImpl(problemURI, RDF.type, QR.QualityProblem));
		sList.add(new StatementImpl(problemURI, QR.isDescribedBy, metricURI));

		if (problemList.getProblemList().get(0) instanceof Quad){
			for(Object obj : problemList.getProblemList()){
				Resource bNode = Commons.generateRDFBlankNode().asResource();
				sList.add(new StatementImpl(problemURI, QR.problematicThing, bNode));
				
				Quad q = (Quad) obj;
				sList.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
				sList.add(new StatementImpl(bNode, RDF.subject, Commons.asRDFNode(q.getSubject())));
				sList.add(new StatementImpl(bNode, RDF.predicate, Commons.asRDFNode(q.getPredicate())));
				sList.add(new StatementImpl(bNode, RDF.object, Commons.asRDFNode(q.getObject())));
				
				if (q.getGraph() != null){
					//TODO: Add a triple for possible Graph instances ()
					
				}
				
			}
		} else {
			Seq problemSeq = ModelFactory.createDefaultModel().createSeq();
			int i = 1;
			for(Object obj : problemList.getProblemList()){
				Resource r = (Resource) obj;
				problemSeq.add(i , r);
				i++;
			}
			sList.add(new StatementImpl(problemURI, QR.problematicThing, problemSeq));
		}
		return sList;
	}

	/**
	 * Create instance triples corresponding towards a Quality Report
	 * 
	 * @param computedOn - The resource URI of the dataset computed on
	 * @param problemReportURIs - A list of quality problem URI instances
	 * 
	 * @return A list of statements corresponding to a quality report
	 */
	public List<Statement> createQualityReport(Resource computedOn, List<Resource> problemReportURIs){
		List<Statement> sList = new ArrayList<Statement>();
		
		Resource reportURI = Commons.generateURI();
		sList.add(new StatementImpl(reportURI, RDF.type, QR.QualityReport));
		sList.add(new StatementImpl(reportURI, QR.computedOn, computedOn));
		for(Resource r : problemReportURIs){
			sList.add(new StatementImpl(reportURI, QR.hasProblem, r));
		}
		return sList;
	}
	
	/**
	 * Returns the URI for a quality problem instance
	 * 
	 * @param problemReport - List of statements corresponding to a problem report
	 * 
	 * @return The resource URI
	 */
	public Resource getProblemURI(List<Statement> problemReport){
		return problemReport.get(0).getSubject();
	}
}
