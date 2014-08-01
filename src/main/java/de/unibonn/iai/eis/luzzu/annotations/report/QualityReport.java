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

public class QualityReport {
	
	public List<Statement> createQualityProblem(Resource metricURI, ProblemList<?> problemList){
		
		List<Statement> sList = new ArrayList<Statement>();
		
		Resource problemURI = Commons.generateURI();
		
		// Defining type
		sList.add(new StatementImpl(problemURI, RDF.type, QR.QualityProblem));
		// Described By
		sList.add(new StatementImpl(problemURI, QR.isDescribedBy, metricURI));
		// Defining a problematicThing

		if (problemList.getProblemList().get(0) instanceof Quad){
			// create reificated RDF
			for(Object obj : problemList.getProblemList()){
				Resource bNode = Commons.generateRDFBlankNode().asResource();
				sList.add(new StatementImpl(problemURI, QR.problematicThing, bNode));
				
				Quad q = (Quad) obj;
				sList.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
				sList.add(new StatementImpl(bNode, RDF.subject, Commons.asRDFNode(q.getSubject())));
				sList.add(new StatementImpl(bNode, RDF.predicate, Commons.asRDFNode(q.getPredicate())));
				sList.add(new StatementImpl(bNode, RDF.object, Commons.asRDFNode(q.getObject())));
				
				if (q.getGraph() != null){
					//sList.add(new StatementImpl(bNode, , Commons.asRDFNode(q.getGraph())));
					//TODO: Add Graph
				}
				
			}
		} else {
			// create seq
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

	public List<Statement> createQualityReport(Resource computedOn, List<Resource> problemReportURIs){
		List<Statement> sList = new ArrayList<Statement>();
		
		// Define QualityReport
		Resource reportURI = Commons.generateURI();
		sList.add(new StatementImpl(reportURI, RDF.type, QR.QualityReport));
		
		// Define on what it is computed
		sList.add(new StatementImpl(reportURI, QR.computedOn, computedOn));
		
		// Define the problems
		for(Resource r : problemReportURIs){
			sList.add(new StatementImpl(reportURI, QR.hasProblem, r));
		}
		
		return sList;
	}
	
	public Resource getProblemURI(List<Statement> problemReport){
		return problemReport.get(0).getSubject();
	}
}
