package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.annotations.QualityMetadata;
import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.cache.CacheManager;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

/**
 * @author Jeremy Debattista
 *
 *	http://jena.apache.org/documentation/io/rdf-input.html
 */
public class StreamProcessor implements IOProcessor {
	
	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String graphCacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	private Map<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	private ExternalMetricLoader loader = ExternalMetricLoader.getInstance();

	private String datasetURI;
	private boolean genQualityReport;
	private Model metricConfiguration;
	private Model qualityReport;
	
	private PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	
	private ExecutorService executor = Executors.newSingleThreadExecutor(); // PipedRDFStream and PipedRDFIterator need to be on different threads

	private boolean isInitalised = false;
	
	
	public StreamProcessor(String datasetURI, boolean genQualityReport, Model configuration){
    	

		this.datasetURI = datasetURI;
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
		cacheMgr.createNewCache(graphCacheName, 50);
		
		// start workflow
		this.setUpProcess();
		try {
			this.startProcessing();
		} catch (ProcessorNotInitialised e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.generateQualityMetadata();
		if (this.genQualityReport) this.generateQualityReport();
	}


	@SuppressWarnings("unchecked")
	public void setUpProcess() {
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);

		// Here we are creating an PipedRDFStream, which accepts triples or quads
		// according to the dataset. A PipedRDFIterator is also created to consume 
		// the input accepted by the PipedRDFStream.
		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			this.iterator = new PipedRDFIterator<Quad>();
			this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			this.iterator = new PipedRDFIterator<Triple>();
			this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
		}
		
		this.isInitalised = true;
		
		
		// load metrics 
		try {
			this.loadMetrics();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void startProcessing() throws ProcessorNotInitialised{
		//TODO: if setUp is not called, throw an error
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		Runnable parser = new Runnable(){
			public void run() {
				RDFDataMgr.parse(rdfStream, datasetURI);
			}
		};
		
		executor.submit(parser); 

		while (this.iterator.hasNext()){
			Object2Quad stmt = new Object2Quad(this.iterator.next());
			sniffer.sniff(stmt.getStatement());
			
			for(String className : this.metricInstances.keySet()){
				QualityMetric m = this.metricInstances.get(className);
				m.compute(stmt.getStatement());
			}
		}
		
		if (sniffer.getCachingObject() != null){
			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
		}
	}


	public void cleanUp() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");
		
		if (!this.executor.isShutdown()){
			this.executor.shutdown();
		}
	}
	
	private void loadMetrics() throws InstantiationException, IllegalAccessException, MalformedURLException, ClassNotFoundException{
		NodeIterator iter = metricConfiguration.listObjectsOfProperty(LMI.metric);
		Map<String, Class<? extends QualityMetric>> map = loader.getQualityMetricClasses();
		
		while(iter.hasNext()){
			String className = iter.next().toString();
			Class<? extends QualityMetric> clazz = map.get(className);
			QualityMetric metric = clazz.newInstance();
			metricInstances.put(className, metric);
		}
		
	}

	private void generateQualityReport() {
		QualityReport r = new QualityReport();
		List<Model> qualityProblems = new ArrayList<Model>();
		
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			qualityProblems.add(r.createQualityProblem(m.getMetricURI(), m.getQualityProblems()));
		}
		
		Resource res = ModelFactory.createDefaultModel().createResource(this.datasetURI);
		this.qualityReport = r.createQualityReport(res, qualityProblems);
	}
	
	private void generateQualityMetadata(){
		Resource res = ModelFactory.createDefaultModel().createResource(this.datasetURI);
		
		QualityMetadata md = new QualityMetadata(res, false);
		
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			md.addMetricData(m);
		}
		
		try {
			Dataset ds = md.createQualityMetadata();
		} catch (MetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Model retreiveQualityReport(){
		return this.qualityReport;
	}


}
