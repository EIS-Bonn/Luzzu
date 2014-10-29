package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import de.unibonn.iai.eis.luzzu.annotations.QualityMetadata;
import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.cache.CacheManager;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.ExternalMetricLoaderException;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.DeclerativeMetricCompiler;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.io.util.CountLatch;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.qml.parser.ParseException;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

public class SparkStreamProcessor implements IOProcessor {

	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String graphCacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	private ConcurrentMap<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	private ExternalMetricLoader loader = ExternalMetricLoader.getInstance();
	private DeclerativeMetricCompiler dmc  = DeclerativeMetricCompiler.getInstance();

	final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);

	private String datasetURI;
	private boolean genQualityReport;
	private Model metricConfiguration;
	private Model qualityReport;
	
	// Processor
	private ExecutorService executor = Executors.newSingleThreadExecutor(); 
	
	
	
	private ExecutorService metricThreadPool = Executors.newCachedThreadPool();
	private final CountLatch metricThreadLatch = new CountLatch(0);


	private boolean isInitalised = false;
	
	public SparkStreamProcessor(String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetURI = datasetURI;
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
		cacheMgr.createNewCache(graphCacheName, 50);
		
		PropertyManager.getInstance().addToEnvironmentVars("datasetURI", datasetURI);
	}
	
	public void processorWorkFlow(){
		this.setUpProcess();
		try {
			this.startProcessing();
		} catch (ProcessorNotInitialised e) {
			this.processorWorkFlow();
		}
		
		this.generateQualityMetadata();
		if (this.genQualityReport) this.generateQualityReport();
	}


	public void setUpProcess() {
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);

		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
		} else {
		}
		
		this.isInitalised = true;
		
		try {
			this.loadMetrics();
		} catch (ExternalMetricLoaderException e) {
			logger.error(e.getLocalizedMessage());
		}
	}

	int counter = 0;
	
	
	public void startProcessing() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		
		SparkProcessor.isParsing = true;
		Runnable parser = new Runnable(){
			public void run() {
				SparkProcessor.parse(datasetURI);
			}
		};
		
		executor.submit(parser);		
		
		while (SparkProcessor.isParsing){
			while (!(SparkProcessor.getProducerQueue().isEmpty())){
				Object2Quad stmt = new Object2Quad(SparkProcessor.pollProducerQueue());
				sniffer.sniff(stmt.getStatement());
				for(String className : this.metricInstances.keySet()){
					logger.debug("Statement with triple <{}> passed to metric {}", stmt.getStatement().asTriple().toString(), className);
					this.metricThreadLatch.increment();
					this.metricThreadPool.submit(new MetricThread(this.metricInstances.get(className), stmt));
				}
			}
		}
		
		try {
			this.metricThreadLatch.awaitZero();
		} catch (InterruptedException e) {
			logger.error("Exception on metric assessment calculation : {}",e.getLocalizedMessage());
		}
				
		if (sniffer.getCachingObject() != null){
			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
		}
	}

	
	public void cleanUp() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");
		
		if (!this.executor.isShutdown()){
			this.executor.shutdown();
			this.metricThreadPool.shutdown();
		}
	}
	
	private void loadMetrics() throws ExternalMetricLoaderException {
		NodeIterator iter = metricConfiguration.listObjectsOfProperty(LMI.metric);
		Map<String, Class<? extends QualityMetric>> map = loader.getQualityMetricClasses();
		
		//Add declerative instance classes
		//TODO better exception handling
		try {
			map.putAll(this.dmc.compile());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		while(iter.hasNext()){
			String className = iter.next().toString();
			Class<? extends QualityMetric> clazz = map.get(className);
			QualityMetric metric = null;
			try {
				metric = clazz.newInstance();
			} catch (InstantiationException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance for " + className + ". Exception caused by an Instantiation Exception : " + e.getLocalizedMessage());
			} catch (IllegalAccessException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance " + className + ". Exception caused by an Illegal Access Exception : " + e.getLocalizedMessage());
			}
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
	
	public void generateQualityMetadata(){
		System.out.println(this.metricThreadLatch.toString());
		Resource res = ModelFactory.createDefaultModel().createResource(this.datasetURI);
		
		QualityMetadata md = new QualityMetadata(res, false);
		
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			md.addMetricData(m);
		}
		
		try {
			RDFDataMgr.write(System.out, md.createQualityMetadata(), Lang.TRIG);
		} catch (MetadataException e) {
			logger.error(e.getMessage());
		}
	}
	
	public Model retreiveQualityReport(){
		return this.qualityReport;
	}

	private final class MetricThread implements Runnable {
        QualityMetric m;
        Object2Quad stmt;
        
        MetricThread(QualityMetric m, Object2Quad stmt) { 
        	this.m = m;
        	this.stmt = stmt;
        }
        
        public void run() {
        	m.compute(stmt.getStatement());
			metricThreadLatch.decrement();
        }
        
    }
}