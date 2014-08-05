package de.unibonn.iai.eis.luzzu.annotations;

import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;
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
	 * @return A Quality Problem RDF Model
	 */
	public Model createQualityProblem(Resource metricURI, ProblemList<?> problemList){
		Model m = ModelFactory.createDefaultModel();
	
		if (problemList.getProblemList().get(0) instanceof Quad){
			for(Object obj : problemList.getProblemList()){
				Resource problemURI = Commons.generateURI();
				
				m.add(new StatementImpl(problemURI, RDF.type, QR.QualityProblem));
				m.add(new StatementImpl(problemURI, QR.isDescribedBy, metricURI));
				
				Resource bNode = Commons.generateRDFBlankNode().asResource();
				m.add(new StatementImpl(problemURI, QR.problematicThing, bNode));
				
				Quad q = (Quad) obj;
				m.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
				m.add(new StatementImpl(bNode, RDF.subject, Commons.asRDFNode(q.getSubject())));
				m.add(new StatementImpl(bNode, RDF.predicate, Commons.asRDFNode(q.getPredicate())));
				m.add(new StatementImpl(bNode, RDF.object, Commons.asRDFNode(q.getObject())));
				
				if (q.getGraph() != null){
					m.add(new StatementImpl(bNode, QR.inGraph, Commons.asRDFNode(q.getGraph())));
				}
				
			}
		} else {
			Seq problemSeq = m.createSeq();
			int i = 1;
			for(Object obj : problemList.getProblemList()){
				Resource r = (Resource) obj;
				problemSeq.add(i , r);
				i++;
			}
			Resource problemURI = Commons.generateURI();
			
			m.add(new StatementImpl(problemURI, RDF.type, QR.QualityProblem));
			m.add(new StatementImpl(problemURI, QR.isDescribedBy, metricURI));
			
			m.add(new StatementImpl(problemURI, QR.problematicThing, problemSeq));
		}
		return m;
	}

	/**
	 * Create instance triples corresponding towards a Quality Report
	 * 
	 * @param computedOn - The resource URI of the dataset computed on
	 * @param problemReport - A list of quality problem as RDF Jena models
	 * 
	 * @return A Jena Model which can be queried or stored
	 */
	public Model createQualityReport(Resource computedOn, List<Model> problemReportModels){
		Model m = ModelFactory.createDefaultModel();
		
		Resource reportURI = Commons.generateURI();
		m.add(new StatementImpl(reportURI, RDF.type, QR.QualityReport));
		m.add(new StatementImpl(reportURI, QR.computedOn, computedOn));
		for(Model prModel : problemReportModels){
			for(Resource r : getProblemURI(prModel)){
				m.add(new StatementImpl(reportURI, QR.hasProblem, r));
				m.add(prModel);
			}
		}
		return m;
	}
	
	/**
	 * Returns the URI for a quality problem instance
	 * 
	 * @param problemReport - A Problem Report Model
	 * 
	 * @return The resource URI
	 */
	public List<Resource> getProblemURI(Model problemReport){
		return problemReport.listSubjectsWithProperty(RDF.type, QR.QualityProblem).toList();
	}
}
