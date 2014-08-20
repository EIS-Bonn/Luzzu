package de.unibonn.iai.eis.luzzu.semantics.utilities;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

public class DAQHelper {

	private DAQHelper(){}
	
	public static String getClassLabel(Resource uri){
		StmtIterator iter = InternalModelConf.getDAQModel().listStatements(uri, RDFS.label, (RDFNode) null);
		String label = "";
		while (iter.hasNext()){
			label = iter.nextStatement().getObject().toString();
		}
		return label;
	}
	
	public static String getDimensionLabel(Resource metricURI){
		return getClassLabel(getDomainResource(metricURI));
	}
	
	public static String getCategoryLabel(Resource metricURI){
		Resource dim = getDomainResource(metricURI);
		Resource cat = getDomainResource(dim);
		
		return getClassLabel(cat);
	}
	
	public static Resource getDimensionResource(Resource metricURI){
		return getDomainResource(metricURI);
	}
		
	public static Resource getCategoryResource(Resource metricURI){
		Resource intermediate = getDomainResource(metricURI);
		return getDomainResource(intermediate);
	}
	
	private static Resource getDomainResource(Resource uri){
		String whereClause = "?prop " + " " + SPARQLHelper.toSPARQL(RDFS.range) + SPARQLHelper.toSPARQL(uri) + " . ";
		whereClause = whereClause + " ?prop " + SPARQLHelper.toSPARQL(RDFS.domain) + " ?domain .";
		
		Model m = InternalModelConf.getFlatModel();
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?domain").replace("[whereClauses]", whereClause);
		Resource r = null;
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();

	    while (rs.hasNext()){
	    	r = rs.next().get("domain").asResource();
	    }
	    
	    return r;
	}
	
	public static Resource getPropertyResource(Resource uri){
		String whereClause = "?prop " + " " + SPARQLHelper.toSPARQL(RDFS.range) + SPARQLHelper.toSPARQL(uri) + " . ";
		
		Model m = InternalModelConf.getFlatModel();
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?prop").replace("[whereClauses]", whereClause);
		Resource r = null;
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	r = rs.next().get("prop").asResource();
	    }
	    
	    return r;
	}
	
	public static String getClassDescription(Resource uri){
		StmtIterator iter = InternalModelConf.getDAQModel().listStatements(uri, RDFS.comment, (RDFNode) null);
		String label = "";
		while (iter.hasNext()){
			label = iter.nextStatement().getObject().toString();
		}
		return label;
	}
	
	/**
	 * Returns the number of metrics computed on a dataset
	 *  
	 * @param Model of the quality metadata in the dataset
	 * @return Returns the number of metrics in a dataset
	 */
	public static int getNumberOfMetricsInDataSet(Model m){
		Integer total = 0;
		
		Model internal = InternalModelConf.getFlatModel();
		Dataset _temp = new DatasetImpl(internal);
		String _tempGraph = Commons.generateURI().toString();
		_temp.addNamedModel(_tempGraph, m);
		
		String whereDefaultGraphClause = "?metricTypeURI " + SPARQLHelper.toSPARQL(RDFS.subClassOf) + " " + SPARQLHelper.toSPARQL(DAQ.Metric) + " .";
		String graphClause = "GRAPH <"+_tempGraph+"> { [where] }";
		String whereNamedGraphClause = "?typeURI " + SPARQLHelper.toSPARQL(RDF.type) + " ?metricTypeURI . ";
		graphClause = graphClause.replace("[where]", whereNamedGraphClause);
		
		String whereClause = whereDefaultGraphClause + graphClause;
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "(count(?typeURI) as ?count)").replace("[whereClauses]", whereClause);

		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, _temp);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	QuerySolution soln = rs.next();
	    	total = new Integer(soln.getResource("count").toString());
	    }
		
		return total.intValue();
	}
}
