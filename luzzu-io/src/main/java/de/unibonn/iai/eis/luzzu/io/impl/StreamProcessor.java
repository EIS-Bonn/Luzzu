package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.annotations.QualityMetadata;
import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.assessment.ComplexQualityMetric;
import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.cache.CacheManager;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.ExternalMetricLoaderException;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.DeclerativeMetricCompiler;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.properties.EnvironmentProperties;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.qml.parser.ParseException;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

/**
 * @author Jeremy Debattista
 *
 *	http://jena.apache.org/documentation/io/rdf-input.html
 */
public class StreamProcessor implements IOProcessor {
	
	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String graphCacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	private ConcurrentMap<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	private ExternalMetricLoader loader = ExternalMetricLoader.getInstance();
	private DeclerativeMetricCompiler dmc  = DeclerativeMetricCompiler.getInstance();

	final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);

	private List<String> datasetList;
	private String datasetURI;
	private boolean genQualityReport;
	private Model metricConfiguration;
	private Model qualityReport;
	
	protected PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
		
	private ExecutorService executor;
	private List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();

	
	private boolean isInitalised = false;
			
	public StreamProcessor(String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetList = new ArrayList<String>();
		this.datasetList.add(datasetURI);
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
		cacheMgr.createNewCache(graphCacheName, 50);
		
		PropertyManager.getInstance().addToEnvironmentVars("datasetURI", datasetURI);
	}
	
	public StreamProcessor(String baseURI, List<String> datasetList, boolean genQualityReport, Model configuration){
		this.datasetList = datasetList;
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
		cacheMgr.createNewCache(graphCacheName, 50);
		
		PropertyManager.getInstance().addToEnvironmentVars("datasetURI", baseURI);
	}
	
	public void processorWorkFlow(){
		this.setUpProcess();
		
		int datasetListCounter = 0;
		for (String dataset : datasetList){
			this.datasetURI = dataset;
			try {
				this.startProcessing();
			} catch (ProcessorNotInitialised e) {
				this.processorWorkFlow();
			}
			datasetListCounter++;
			if (datasetListCounter < datasetList.size()) this.reinitialiseProcessors();
		}
		
		this.generateQualityMetadata();
		if (this.genQualityReport) this.generateQualityReport();
	}

	@SuppressWarnings("unchecked")
	private void reinitialiseProcessors(){
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
		
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);
		
		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			this.iterator = new PipedRDFIterator<Quad>();
			this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			this.iterator = new PipedRDFIterator<Triple>();
			this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
		}
		
		this.isInitalised = true;
		
		this.executor = Executors.newSingleThreadExecutor();
		
		lstMetricConsumers = new ArrayList<MetricProcess>();
		for(String className : this.metricInstances.keySet()) {
			this.lstMetricConsumers.add(new MetricProcess(this.metricInstances.get(className)));
		}
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
			
			this.executor = Executors.newSingleThreadExecutor();
	}
	
	public void cleanUp() throws ProcessorNotInitialised{
		
		this.isInitalised = false;
		
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
				
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
	}

	public void startProcessing() throws ProcessorNotInitialised{
				
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		Runnable parser = new Runnable(){
			public void run() {
				RDFDataMgr.parse(rdfStream, datasetURI);
			}
		};
		
		executor.submit(parser);
		
		try {
			while (this.iterator.hasNext()) {
				
				Object2Quad stmt = new Object2Quad(this.iterator.next());
				sniffer.sniff(stmt.getStatement());
				
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
		
		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
		}
		
		for(String clazz : metricInstances.keySet()){
			if(metricInstances.get(clazz) instanceof ComplexQualityMetric){
				((ComplexQualityMetric)metricInstances.get(clazz)).after();
			}
			metricInstances.get(clazz).metricValue();
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
			if (this.metricInstances.get(className) instanceof ComplexQualityMetric){
				((ComplexQualityMetric)this.metricInstances.get(className)).before();
			}
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
		
		Resource res = null;
		try {
			res = ModelFactory.createDefaultModel().createResource(EnvironmentProperties.getInstance().getDatasetURI());
		} catch (Exception e) {
			logger.error("Dataset Exception " + e.getLocalizedMessage());
		}
		this.qualityReport = r.createQualityReport(res, qualityProblems);
	}
	
	private void generateQualityMetadata(){
		Resource res = null;
		try {
			res = ModelFactory.createDefaultModel().createResource(EnvironmentProperties.getInstance().getDatasetURI());
		} catch (Exception e1) {
			logger.error("Dataset Exception " + e1.getLocalizedMessage());
		}
		
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
		volatile Queue<Object2Quad> quadsToProcess = new BlockingArrayQueue<Object2Quad>(10000000);
		Thread metricThread = null;
		String metricName = null;
        
        Integer stmtsProcessed = 0;
        boolean stopSignal = false;
        
        MetricProcess(final QualityMetric m) { 
        	
        	this.metricName = m.getClass().getSimpleName();
        	        	
        	this.metricThread = (new Thread() { 
        		@Override
        		public void run() {
        			logger.debug("Starting thread for metric {}", m.getClass().getName());
        			
        			Object2Quad curQuad = null;
        			
        			while(!stopSignal || !quadsToProcess.isEmpty()) {
        				
        				curQuad = quadsToProcess.poll();
        				
        				if(curQuad != null) {
        					logger.trace("Metric {}, new quad (processed: {}, to-process: {})", m.getClass().getName(), stmtsProcessed, quadsToProcess.size());
        					
	        				m.compute(curQuad.getStatement());
	        				
	        				curQuad = null;
	            			stmtsProcessed++;
        				}
        			}
        			logger.debug("Thread for metric {} completed, total statements processed {}", m.getClass().getName(), stmtsProcessed);
        		}
        		
        	});
        	
        	this.metricThread.start();
        }

		public void notifyNewQuad(Object2Quad newQuad) {
			quadsToProcess.add(newQuad);
			logger.trace("Metric {}, element added to queue (to-process: {})", this.metricName, quadsToProcess.size());
		}
		
		public void stop() {
			while(!quadsToProcess.isEmpty()) {
				logger.debug("Waiting for items on queue: {} Metric: {}", quadsToProcess.size(), this.metricName);
			}
			
			this.stopSignal = true;
		}

    }

}
