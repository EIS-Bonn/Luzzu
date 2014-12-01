package de.unibonn.iai.eis.luzzu.semantics.utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;
import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
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
	 * Extract all observations from a dataset
	 * @param dataset URI
	 * @return a HashMap with the metric type being the key and a list of observations
	 */
	public static Map<String,List<Observation>> getQualityMetadataObservations(String datasetMetadataUri){
		Dataset d = RDFDataMgr.loadDataset(datasetMetadataUri);
		Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
		Model qualityMD = d.getNamedModel(graph.getURI());
		
		Map<String,List<Observation>> map = new HashMap<String,List<Observation>>(); // metric resource, list<observations>
		
		ResIterator iter = qualityMD.listResourcesWithProperty(RDF.type, CUBE.Observation);
		while(iter.hasNext()){
			Resource res = iter.next();
			
			//get metric uri
			Resource metricURI = qualityMD.listObjectsOfProperty(res, DAQ.metric).next().asResource();
			//get metric type
			Resource metricType = qualityMD.listObjectsOfProperty(metricURI, RDF.type).next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, DC.date).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//get value
			Float value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getFloat();
			
			
			Observation obs = new Observation(res, date, value, metricType);
			
			if (!(map.containsKey(metricType.toString()))){
				map.put(metricType.toString(), new ArrayList<Observation>());
			}
			map.get(metricType.toString()).add(obs);
		}
		
		return map;
	}
	
	/**
	 * Queries the internal model for the total number of metrics in each dimension.
	 * 
	 * @return a Map of dimensions and the count of metrics.
	 */
	public static Map<String, Integer> getNumberOfMetricsInDimension(){
		Map<String, Integer> metricsPerDimension = new HashMap<String, Integer>();
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?dimensionURI a " + SPARQLHelper.toSPARQL(DAQ.Dimension) + 
				" . ?dimensionURI ?hasMetricProperty ?metricURI . " +
				"?hasMetricProperty " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasMetric) + " .";
		
		String variables = "?dimensionURI COUNT(?metricURI) as ?count";
		
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", variables).replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	String dim = qs.get("dimensionURI").asResource().getURI();
	    	Integer count = qs.get("count").asLiteral().getInt();
	    	metricsPerDimension.put(dim, count);
	    }
		
	    return metricsPerDimension;
	}
	
	public static String getDimensionForMetric(Resource metricURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?dimension ?prop " + SPARQLHelper.toSPARQL(metricURI) +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasMetric) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?dimension").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    String dim = "";
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	dim = qs.get("dimension").asResource().getURI();
	    }
		
	    return dim;
	}
	
	public static Map<String, Integer> getNumberOfDimensionsInCategory(){
		Map<String, Integer> dimensionPerCategory = new HashMap<String, Integer>();
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?categoryURI a " + SPARQLHelper.toSPARQL(DAQ.Category) + 
				" . ?categoryURI ?hasDimensionProperty ?dimensionURI . " +
				"?hasMetricProperty " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
		
		String variables = "?categoryURI COUNT(?dimensionURI) as ?count";
		
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", variables).replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	String cat = qs.get("categoryURI").asResource().getURI();
	    	Integer count = qs.get("count").asLiteral().getInt();
	    	dimensionPerCategory.put(cat, count);
	    }
		
	    return dimensionPerCategory;
	}
	
	public static String getCategoryForDimension(Resource dimensionURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?category ?prop " + SPARQLHelper.toSPARQL(dimensionURI) +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?category").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    String cat = "";
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	cat = qs.get("category").asResource().getURI();
	    }
		
	    return cat;
	}
	
	public static List<String> getDimensionsInCategory(Resource categoryURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = SPARQLHelper.toSPARQL(categoryURI) + " ?prop ?dimensionURI" +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?dimensionURI").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    List<String> dimensions = new ArrayList<String>();
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	dimensions.add(qs.get("dimensionURI").asResource().getURI());
	    }
		
	    return dimensions;
	}
	
	/**
	 * Formats an xsd:dateTime to a JAVA object data
	 * @param date - A string extracted from the triple's object
	 * @return JAVA object data
	 * 
	 * @throws ParseException
	 */
	private static Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		return sdf.parse(date);
	}

	
	
	
	
	
	@Deprecated
	public static int getNumberOfMetricsInDataSet(Model m){
		return getNumberOfMetricsInDataSet(m, "");
	}
	
	@Deprecated
	public static int getNumberOfMetricsInDataSet(Model m, Resource dimensionOrCategoryURI, boolean isCategory){
		String extraWhereClause = "";
		
		if (isCategory){
			extraWhereClause = SPARQLHelper.toSPARQL(dimensionOrCategoryURI) + " ?hasDimensionProperty ?dimensionTypeURI . ";
			extraWhereClause = "?dimensionTypeURI  ?hasMetricProperty ?metricTypeURI .";
		} else {
			extraWhereClause = SPARQLHelper.toSPARQL(dimensionOrCategoryURI) + " ?hasMetricProperty ?metricTypeURI . ";
		}
		
		return getNumberOfMetricsInDataSet(m, extraWhereClause);
	}
	
	@Deprecated
	private static int getNumberOfMetricsInDataSet(Model m, String extraSPARQLstmt){
		Integer total = 0;
		
		Model internal = InternalModelConf.getFlatModel();
		Dataset _temp = new DatasetImpl(internal);
		String _tempGraph = Commons.generateURI().toString();
		_temp.addNamedModel(_tempGraph, m);
		
		String whereDefaultGraphClause = "?metricTypeURI " + SPARQLHelper.toSPARQL(RDFS.subClassOf) + " " + SPARQLHelper.toSPARQL(DAQ.Metric) + " .";
		whereDefaultGraphClause = whereDefaultGraphClause + extraSPARQLstmt;
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
