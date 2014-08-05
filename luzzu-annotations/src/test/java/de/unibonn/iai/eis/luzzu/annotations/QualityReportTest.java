package de.unibonn.iai.eis.luzzu.annotations;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;
import de.unibonn.iai.eis.luzzu.exceptions.ProblemListInitialisationException;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.QR;

public class QualityReportTest extends Assert {

	private QualityReport qr = new QualityReport();
	private Model datasetSample = ModelFactory.createDefaultModel();
	private ProblemList<Quad> quadProblemList;
	private ProblemList<Resource> resourceProblemList;
	
	private Resource metricURI = datasetSample.createResource("urn:metric/Capitalisation789");
	private Resource computedOn = datasetSample.createResource("ex:qrtest.trig");
	private Resource temporaryURI1;
	private Resource temporaryURI2;
	
	@Before
	public void setUp() throws Exception {
		this.populateProblemLists();
	}
	
	@After
	public void tearDown() throws Exception{
	}
	
	@Test
	public void generateQuadQualityProblem(){
		Model qualityProblem = qr.createQualityProblem(metricURI, quadProblemList);

		// checks if model contains appropriate data 
		assertEquals(14, qualityProblem.size()); 
		
		assertTrue(qualityProblem.listStatements(null, RDF.type, QR.QualityProblem).hasNext());
		assertTrue(qualityProblem.listStatements(null, QR.isDescribedBy, metricURI).hasNext());
		
		NodeIterator objects = qualityProblem.listObjectsOfProperty(QR.problematicThing);
		assertTrue(objects.hasNext());
		
		RDFNode _node = objects.next();
		assertTrue(_node.asNode().isBlank());
		
		assertTrue(qualityProblem.listStatements(_node.asResource(), RDF.type, RDF.Statement).hasNext());
		assertTrue(qualityProblem.listStatements(_node.asResource(), RDF.subject, (RDFNode) null).hasNext());
		assertTrue(qualityProblem.listStatements(_node.asResource(), RDF.predicate, RDFS.label).hasNext());
		assertTrue(qualityProblem.listStatements(_node.asResource(), RDF.object, (RDFNode) null).hasNext());
	}
	
	@Test
	public void generateSeqQualityProblem(){
		Model qualityProblem = qr.createQualityProblem(metricURI, resourceProblemList);
		
		// checks if model contains appropriate data 
		assertEquals(6, qualityProblem.size()); 
		
		assertTrue(qualityProblem.listStatements(null, RDF.type, QR.QualityProblem).hasNext());
		assertTrue(qualityProblem.listStatements(null, QR.isDescribedBy, metricURI).hasNext());
		
		assertEquals(1, qualityProblem.listObjectsOfProperty(QR.problematicThing).toList().size());
		
		ResIterator seqURI = qualityProblem.listSubjectsWithProperty(RDF.type, RDF.Seq);
		assertEquals(2,qualityProblem.getSeq(seqURI.next()).size());
	}
	
	@Test
	public void generateQualityReportWithQuads(){
		List<Model> qualityProblemModels = new ArrayList<Model>();
		qualityProblemModels.add(qr.createQualityProblem(metricURI, quadProblemList));
		Model qualityReport = qr.createQualityReport(computedOn, qualityProblemModels);
		
		// checks if model contains appropriate data 
		assertEquals(18, qualityReport.size()); 
		
		assertTrue(qualityReport.listStatements(null, RDF.type, QR.QualityReport).hasNext());
		assertTrue(qualityReport.listStatements(null, QR.computedOn, computedOn).hasNext());
	
		assertEquals(2, qualityReport.listObjectsOfProperty(QR.hasProblem).toList().size()); 
	}
	
	@Test
	public void generateQualityReportWithDifferentTypes(){
		List<Model> qualityProblemModels = new ArrayList<Model>();
		qualityProblemModels.add(qr.createQualityProblem(metricURI, quadProblemList));
		qualityProblemModels.add(qr.createQualityProblem(metricURI, resourceProblemList));
		Model qualityReport = qr.createQualityReport(computedOn, qualityProblemModels);
		
		// checks if model contains appropriate data 
		assertEquals(25, qualityReport.size()); 
		
		assertTrue(qualityReport.listStatements(null, RDF.type, QR.QualityReport).hasNext());
		assertTrue(qualityReport.listStatements(null, QR.computedOn, computedOn).hasNext());
	
		assertEquals(3, qualityReport.listObjectsOfProperty(QR.hasProblem).toList().size()); 
	}
	
	private void populateProblemLists() throws ProblemListInitialisationException{
		temporaryURI1 = datasetSample.createResource("ex:Joe");
		Literal joeLiteral = datasetSample.createLiteral("JoeDoe");
		temporaryURI2 = datasetSample.createResource("ex:UniBonn");
		Literal uniBonnLiteral = datasetSample.createLiteral("UniBonn");
		
		List<Quad> lQuad = new ArrayList<Quad>();
		lQuad.add(new Quad(null, temporaryURI1.asNode(), RDFS.label.asNode(), joeLiteral.asNode()));
		lQuad.add(new Quad(null, temporaryURI2.asNode(), RDFS.label.asNode(), uniBonnLiteral.asNode()));
		quadProblemList = new ProblemList<Quad>(lQuad);
		
		List<Resource> lResource = new ArrayList<Resource>();
		lResource.add(temporaryURI1);
		lResource.add(temporaryURI2);
		resourceProblemList = new ProblemList<Resource>(lResource);
	}
}
