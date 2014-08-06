package de.unibonn.iai.eis.luzzu.annotations;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.cache.CacheManager;
import de.unibonn.iai.eis.luzzu.cache.impl.TemporaryGraphMetadataCacheObject;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 * 
 * The Quality Metadata Class provides a number of methods
 * that enables the representation of Quality Metadata
 * as described by the Dataset Quality Ontology (DAQ).
 *
 */
public class QualityMetadata {

	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String cacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	
	private Model metadata = ModelFactory.createDefaultModel();
	private Resource qualityGraph;
	boolean metadataPresent = false;
	private TemporaryGraphMetadataCacheObject _temp = null;
	
	/**
	 * Since each assessed dataset should have only one quality 
	 * metadata graph, we need to check if it already exists
	 * in the cache.
	 * 
	 * @param datasetURI - The assessed dataset
	 * @param sparqlEndpoint - True if the datasetURI is a sparql endpoint.
	 */
	public QualityMetadata(Resource datasetURI, boolean sparqlEndpoint){
		if (sparqlEndpoint){
			//TODO:sparqlendpoint
			//query, do not check in cache as it would not be feasible to store sparql endpoint results in cache
			//if exists set qualityGraphURI
		}
		
		if (cacheMgr.existsInCache(cacheName, datasetURI)){
			_temp = (TemporaryGraphMetadataCacheObject) cacheMgr.getFromCache(cacheName, datasetURI);
			this.qualityGraph = _temp.getGraphURI();
			this.metadata.add(_temp.getMetadataModel());
			this.metadataPresent = true;
		} else {
			this.qualityGraph = Commons.generateURI();
		}
	}
	
	/**
	 * Used when the assessed dataset is stored in memory 
	 * (Jena Model),
	 * 
	 * @param dataset - Assessed Jena Model Dataset
	 */
	public QualityMetadata(Model dataset){
		ResIterator qualityGraphRes = dataset.listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
		if (qualityGraphRes.hasNext()){
			this.qualityGraph = qualityGraphRes.next();
			this.metadataPresent = true;
		} else {
			this.qualityGraph = Commons.generateURI();
		}
	}

	/**
	 * Creates observational data for the assessed metric
	 * 
	 * @param metric - Metric Class
	 */
	public void addMetricData(QualityMetric metric){
		Resource categoryURI = null;
		Resource categoryType = DAQHelper.getCategoryResource(metric.getMetricURI());
		
		if (!(this.categoryExists(categoryType, categoryURI))){
			categoryURI = Commons.generateURI();
			this.metadata.add(categoryURI, RDF.type, categoryType);
		}
		
		Resource dimensionURI = null;
		Resource dimensionType = DAQHelper.getDimensionResource(metric.getMetricURI());
		if (!(this.dimensionExists(dimensionType, dimensionURI))){
			dimensionURI = Commons.generateURI();
			Property dimensionProperty = this.metadata.createProperty(DAQHelper.getPropertyResource(dimensionType).getURI());
			this.metadata.add(categoryURI, dimensionProperty, dimensionURI);
			this.metadata.add(dimensionURI, RDF.type, dimensionType);
		}
		
		Resource metricURI = null;
		Resource metricType = metric.getMetricURI();
		if (!(this.metricExists(metricType, metricURI))){
			metricURI = Commons.generateURI();
			Property metricProperty = this.metadata.createProperty(DAQHelper.getPropertyResource(metricType).getURI());
			this.metadata.add(dimensionURI, metricProperty, metricURI);
			this.metadata.add(metricURI, RDF.type, metricType);
		}
		
		Resource observationURI = Commons.generateURI();
		this.metadata.add(metricURI, DAQ.hasObservation, observationURI);
		
		this.metadata.add(observationURI, RDF.type, CUBE.Observation);
		this.metadata.add(observationURI, DC.date, Commons.generateCurrentTime());
		this.metadata.add(observationURI, DAQ.metric, metricURI);
		
		this.metadata.add(observationURI, DAQ.value, Commons.generateDoubleTypeLiteral(metric.metricValue()));
		
		this.metadata.add(observationURI, CUBE.dataSet, qualityGraph);
	}
	
	/**
	 * Creates quality metadata
	 * 
	 * @return Dataset with quality metadata which needs to be attached to the assessed dataset.
	 * @throws MetadataException if there is no observation data calculated.
	 */
	public Dataset createQualityMetadata() throws MetadataException{
		Model defaultModel = ModelFactory.createDefaultModel();
		Dataset dataset = null;
		
		if (this.metadata.size() == 0) throw new MetadataException("No Metric Observations Recorded");
		
		if (!metadataPresent){
			defaultModel.add(qualityGraph, RDF.type, DAQ.QualityGraph);
			defaultModel.add(qualityGraph, CUBE.structure, DAQ.dsd);
			dataset = new DatasetImpl(defaultModel);
		}
		dataset.addNamedModel(this.qualityGraph.getURI(), this.metadata);
		
		return dataset;
	}
	
	/**
	 * Checks if a category uri exists in the metadata
	 * 
	 * @param categoryType - The URI of the Category Type
	 * @param categoryURI - An instance which will be overridden by this method
	 * @return True if exists
	 */
	private boolean categoryExists(Resource categoryType, Resource categoryURI){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, categoryType);
		if (resIte.hasNext()){
			categoryURI = resIte.next();
			return true;
		}
		return false;
	}
	
	/**
	 * Checks if a dimension uri exists in the metadata
	 * 
	 * @param dimensionType - The URI of the Dimension Type
	 * @param dimensionURI - An instance which will be overridden by this method
	 * @return True if exists
	 */
	private boolean dimensionExists(Resource dimensionType, Resource dimensionURI){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, dimensionType);
		if (resIte.hasNext()){
			dimensionURI = resIte.next();
			return true;
		}
		return false;
	}
	
	/**
	 * Checks if a metric uri exists in the metadata
	 * 
	 * @param metricType - The URI of the Metric Type
	 * @param metricURI - An instance which will be overridden by this method
	 * @return True if exists
	 */
	private boolean metricExists(Resource metricType, Resource metricURI){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, metricType);
		if (resIte.hasNext()){
			metricURI = resIte.next();
			return true;
		}
		return false;
	}
}
