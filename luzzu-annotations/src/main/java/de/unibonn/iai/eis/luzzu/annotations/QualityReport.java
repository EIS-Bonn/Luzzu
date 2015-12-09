package de.unibonn.iai.eis.luzzu.annotations;

import java.io.File;
import java.util.List;
import java.util.UUID;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Seq;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.QPRO;
import de.unibonn.iai.eis.luzzu.cache.JenaCacheObject;
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
 * @see src/main/resource/vocabularies/QPRO/QPRO.trig
 * 
 */
public class QualityReport {
	
	protected String TDB_DIRECTORY = PropertyManager.getInstance().getProperties("directories.properties").getProperty("TDB_TEMP_BASE_DIR")+"tdb_"+UUID.randomUUID().toString()+"/";
	protected Dataset dataset = TDBFactory.createDataset(TDB_DIRECTORY);
	
	public QualityReport(){
		TDB.sync(dataset);
		dataset.begin(ReadWrite.WRITE);
		dataset.getDefaultModel().removeAll(); // since this TDB is meant to be temporary, then we will remove all statements
	}
	
	
	/**
	 * Creates instance triples corresponding to a quality problem
	 * 
	 * @param metricURI - The metric's instance URI
	 * @param problemList - The list of problematic triples found during the assessment of the metric
	 * 
	 * @return The Temporary Graph URI
	 */
	public String createQualityProblem(Resource metricURI, ProblemList<?> problemList){
		Model m = ModelFactory.createDefaultModel();
		String genGraph = Commons.generateURI().toString();		
		
		Object oneObject = new Object();
		if (problemList.getProblemList().iterator().hasNext())
			oneObject = problemList.getProblemList().iterator().next();
		
		// Validate that there's at least a problematic triple to be reported	
		if (problemList != null && problemList.getProblemList().size() > 0 && (oneObject instanceof Quad)){
			for(Object obj : problemList.getProblemList()){
				Resource problemURI = Commons.generateURI();
				
				m.add(new StatementImpl(problemURI, RDF.type, QPRO.QualityProblem));
				m.add(new StatementImpl(problemURI, QPRO.isDescribedBy, metricURI));
				
				Resource bNode = Commons.generateRDFBlankNode().asResource();
				m.add(new StatementImpl(problemURI, QPRO.problematicThing, bNode));
				
				Object _obj = obj;
				if (obj instanceof JenaCacheObject){
					_obj = ((JenaCacheObject<?>) obj).deserialise();
				}
				Quad q = (Quad) _obj;
				m.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
				m.add(new StatementImpl(bNode, RDF.subject, Commons.asRDFNode(q.getSubject())));
				m.add(new StatementImpl(bNode, RDF.predicate, Commons.asRDFNode(q.getPredicate())));
				m.add(new StatementImpl(bNode, RDF.object, Commons.asRDFNode(q.getObject())));
				
				if (q.getGraph() != null){
					m.add(new StatementImpl(bNode, QPRO.inGraph, Commons.asRDFNode(q.getGraph())));
				}
			}
		} else if (problemList != null && problemList.getProblemList().size() > 0 && oneObject instanceof Model){
			for(Object obj : problemList.getProblemList()){
				Resource problemURI = Commons.generateURI();
				
				m.add(new StatementImpl(problemURI, RDF.type, QPRO.QualityProblem));
				m.add(new StatementImpl(problemURI, QPRO.isDescribedBy, metricURI));
				
				Object _obj = obj;
				if (obj instanceof JenaCacheObject){
					_obj = ((JenaCacheObject<?>) obj).deserialise();
				}
				Model qpModel = (Model) _obj;
				Resource sNode = qpModel.listSubjects().next();
				m.add(new StatementImpl(problemURI, QPRO.problematicThing, sNode));
				m.add(qpModel);
			}
		} else {
			Seq problemSeq = m.createSeq();
			int i = 1;
			for(Object obj : problemList.getProblemList()){
				
				Object _obj = obj;
				if (obj instanceof JenaCacheObject){
					_obj = ((JenaCacheObject<?>) obj).deserialise();
				}
				Resource r = (Resource) _obj;
				problemSeq.add(i , r);
				i++;
			}
			Resource problemURI = Commons.generateURI();
			
			m.add(new StatementImpl(problemURI, RDF.type, QPRO.QualityProblem));
			m.add(new StatementImpl(problemURI, QPRO.isDescribedBy, metricURI));
			
			m.add(new StatementImpl(problemURI, QPRO.problematicThing, problemSeq));
		}
		dataset.addNamedModel(genGraph, m);

		return genGraph;
	}

	/**
	 * Create instance triples corresponding towards a Quality Report
	 * 
	 * @param computedOn - The resource URI of the dataset computed on
	 * @param problemReport - A list of quality problem as RDF Jena models
	 * 
	 * @return A Jena Model which can be queried or stored
	 */
	public Model createQualityReport(Resource computedOn, List<String> problemReportModels){
		Model m = dataset.getDefaultModel();
		
		Resource reportURI = Commons.generateURI();
		m.add(new StatementImpl(reportURI, RDF.type, QPRO.QualityReport));
		m.add(new StatementImpl(reportURI, QPRO.computedOn, computedOn));
		for(String prModelURI : problemReportModels){
			Model prModel = getProblemReportFromTBD(prModelURI);
			for(Resource r : getProblemURI(prModel)){
				m.add(new StatementImpl(reportURI, QPRO.hasProblem, r));
				m.add(prModel);
			}
			dataset.removeNamedModel(prModelURI);
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
		return problemReport.listSubjectsWithProperty(RDF.type, QPRO.QualityProblem).toList();
	}
	
	public Model getProblemReportFromTBD(String problemModelURI){
		return dataset.getNamedModel(problemModelURI);
	}
	
	public void flush(){
		dataset.commit();
		dataset.close();
		
		File f = new File(TDB_DIRECTORY);
		f.delete();
	}
}
