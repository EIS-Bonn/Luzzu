package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Int;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;

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

/**
 * @author Jeremy Debattista
 *
 *	http://jena.apache.org/documentation/io/rdf-input.html
 */
public class StreamProcessorObserver implements IOProcessor {
	
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
	
	protected PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
		
	private ExecutorService executor;
	private ExecutorService metricThreadPool;
	private List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();
	private final CountLatch metricThreadLatch = new CountLatch(0);

	
	private boolean isInitalised = false;
			
	public StreamProcessorObserver(String datasetURI, boolean genQualityReport, Model configuration){
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


	@SuppressWarnings("unchecked")
	public void setUpProcess() {
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);

		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			this.iterator = new PipedRDFIterator<Quad>();
			this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			this.iterator = new PipedRDFIterator<Triple>();
			this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
		}
		
		this.isInitalised = true;
		
		try {
			this.loadMetrics();
		} catch (ExternalMetricLoaderException e) {
			logger.error(e.getLocalizedMessage());
		}
		
		// [slondono] - Create a thread pool intended to run all the metric threads in an individual thread,
		// the pool takes care of mantaining the threads and reusing them for efficiency, the thread pool is initialized 
		// here as we want to make the number of threads equal to the number of metrics to be used and we don't want to instantiate
		// an executor every time startProcessing() is invoked. 
		this.executor = Executors.newSingleThreadExecutor();
		if (this.metricInstances.size() > 0) this.metricThreadPool = Executors.newFixedThreadPool(this.metricInstances.size());
		logger.debug("List of metric threads ready, {} metric tasks added", this.metricInstances.size());
	}

	public void startProcessing() throws ProcessorNotInitialised{
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
//		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		Runnable parser = new Runnable(){
			public void run() {
				RDFDataMgr.parse(rdfStream, datasetURI);
			}
		};
		
		executor.submit(parser);
		
		
		try {
			while (this.iterator.hasNext()) {
				
				Object2Quad stmt = new Object2Quad(this.iterator.next());
//				sniffer.sniff(stmt.getStatement());
				
				if (lstMetricConsumers != null){
					for(MetricProcess mConsumer : lstMetricConsumers) {
						mConsumer.notifyNewQuad(stmt);
					}
				}
			}
		} 
		finally {
			if (lstMetricConsumers != null){
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}	
			}		
		}
		
//		if (sniffer.getCachingObject() != null) {
//			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
//		}
	}


	public void cleanUp() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");
		
		if (!this.executor.isShutdown()){
			this.executor.shutdown();
		}
		
		// [slondono] - Make sure this method is invoked after changing the metric configuration (that is, 
		// before re-invoking setUp() with a different metric configuration, this is to free threads created on previous processings)
		if (this.metricInstances.size() > 0){
			if(!this.metricThreadPool.isShutdown()) {
				// [slondono] - Important! this invocation actually destroy the threads in the pool, freeing the associated resources
				this.metricThreadPool.shutdown();
			}
		}
	}
	
	private void loadMetrics() throws ExternalMetricLoaderException {
		NodeIterator iter = metricConfiguration.listObjectsOfProperty(LMI.metric);
		Map<String, Class<? extends QualityMetric>> map = loader.getQualityMetricClasses();
		
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
		
		for(String className : this.metricInstances.keySet()) {
			this.lstMetricConsumers.add(new MetricProcess(this.metricInstances.get(className)));
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
			RDFDataMgr.write(System.out, md.createQualityMetadata(), Lang.TRIG);
		} catch (MetadataException e) {
			logger.error(e.getMessage());
		}
	}
	
	public Model retreiveQualityReport(){
		return this.qualityReport;
	}

	private final class MetricProcess {

		// [slondono] - The quality metric instance stays alive, even after call has finished, this way accumulating the 
		// results of all invocations of call performed on this instance
		Queue<Object2Quad> quadsToProcess = new BlockingArrayQueue<Object2Quad>(5000);
		Thread metricThread = null;
        
        Integer stmtsProcessed = 0;
        boolean stopSignal = false;
        
        MetricProcess(final QualityMetric m) { 
        	        	
        	this.metricThread = (new Thread() { 
        		@Override
        		public void run() {
        			logger.debug("Starting thread for metric {}", m.getClass().getName());
        			
        			Object2Quad curQuad = null;
        			
        			while(!stopSignal || quadsToProcess.size() > 0) {
        				
        				curQuad = quadsToProcess.poll();
        				
        				if(curQuad != null) {
        					logger.debug("Metric {}, Processing new quad...", m.getClass().getName());
        					
	        				m.compute(curQuad.getStatement());
	        				
	        				curQuad = null;
	            			stmtsProcessed++;
        				}
        			}
        			System.out.println(m.getClass().getName() + " " + stmtsProcessed + "pushed on queue: " + qCount);
        			logger.debug("Thread for metric {} completed, total statements processed {}", m.getClass().getName(), stmtsProcessed);
        		}
        	});
        	
        	this.metricThread.start();
        }

        int qCount = 0;
		public void notifyNewQuad(Object2Quad newQuad) {
			qCount++;
			quadsToProcess.add(newQuad);
		}
		
		public void stop() {
			this.stopSignal = true;
		}

    }

}
