package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
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
import de.unibonn.iai.eis.luzzu.datatypes.Args;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;
import de.unibonn.iai.eis.luzzu.exceptions.AfterException;
import de.unibonn.iai.eis.luzzu.exceptions.BeforeException;
import de.unibonn.iai.eis.luzzu.exceptions.ExternalMetricLoaderException;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.DeclerativeMetricCompiler;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.io.helper.IOStats;
import de.unibonn.iai.eis.luzzu.io.helper.StreamMetadataSniffer;
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
	private final String metadataBaseDir;
	
	private ConcurrentMap<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	private ExternalMetricLoader loader = ExternalMetricLoader.getInstance();
	private DeclerativeMetricCompiler dmc  = DeclerativeMetricCompiler.getInstance();

	final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);
	
	private List<String> datasetList;
	private String baseURI;
	private String datasetURI;
	private boolean genQualityReport;
	private Model metricConfiguration;
	private Model qualityReport;
	
	protected PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	// RDFIterator parameters
	private final int rdfIterBufferSize = PipedRDFIterator.DEFAULT_BUFFER_SIZE * 2;
	private final int rdfIterPollTimeout = 10000;
	private final int rdfIterMaxPolls = 50;
	private final boolean rdfIterFairBufferLock = true;
		
	private ExecutorService executor;
	private List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();
	
	private boolean isInitalised = false;
	private boolean forcedCancel = false;
	
	
	// Stats Vars
	private boolean isGeneratingQMD = false;
	private boolean endedGeneratingQMD = false;
	
	private boolean isGeneratingQR = false;
	private boolean endedGeneratingQR = false;
	
	/**
	 * Default initializations common to all constructors (is always called upon instance creation)
	 */
	{	
		// Initialize cache manager
		cacheMgr.createNewCache(graphCacheName, 50);
		
		// Load properties from configuration files
		PropertyManager props = PropertyManager.getInstance();
		// If the directory to store quality metadata and problem reports was not specified, set it to user's home
		if(props.getProperties("directories.properties") == null || 
				props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR") == null) {
			metadataBaseDir = System.getProperty("user.dir") + "/qualityMetadata";
		} else {
			metadataBaseDir = props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR");
		}
	}
			
	public StreamProcessor(String baseURI, String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetList = new ArrayList<String>();
		this.datasetList.add(datasetURI);
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		this.baseURI = baseURI;
		
		PropertyManager.getInstance().addToEnvironmentVars("baseURI", baseURI);
	}
	
	public StreamProcessor(String baseURI, List<String> datasetList, boolean genQualityReport, Model configuration){
		this.datasetList = datasetList;
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		this.baseURI = baseURI;
		
		PropertyManager.getInstance().addToEnvironmentVars("baseURI", baseURI);
	}
	
	public void processorWorkFlow(){
		this.setUpProcess();
		
		int datasetListCounter = 0;

		for (String dataset : datasetList){
			this.datasetURI = dataset; 
			PropertyManager.getInstance().addToEnvironmentVars("datasetURI", this.datasetURI);
			try {
				this.startProcessing();
			} catch (ProcessorNotInitialised e) {
				logger.warn("Processor not initialized while trying to start dataset processing. Dataset: {}", this.datasetURI);
				this.processorWorkFlow();
			}
			datasetListCounter++;
			if (datasetListCounter < datasetList.size()) this.reinitialiseProcessors();
		}
		
		if (!forcedCancel){
			this.writeQualityMetadataFile();
			// Generate quality report, if required by the invoker and write it into a file
			if (this.genQualityReport) {
				this.generateQualityReport();
				this.writeReportMetadataFile();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void reinitialiseProcessors(){
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
		
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);
		
		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			this.iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			this.iterator = new PipedRDFIterator<Triple>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
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
				this.iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
				this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
			} else {
				this.iterator = new PipedRDFIterator<Triple>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
				this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
			}
			logger.debug("PipedRDFIterator initialized with: Buffer Size {}, Fair Lock {}, Poll Timeout {}, Max Polls {}", 
					rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			
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

	public void startProcessing() throws ProcessorNotInitialised {
				
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		Runnable parser = new Runnable() {
			public void run() {
				try{
					RDFDataMgr.parse(rdfStream, datasetURI);
				} catch (Exception e) {
					logger.error("Error parsing dataset {}. Error message {}", datasetURI, e.getMessage());
				}
			}
		};
		
		executor.submit(parser);
		long totalQuadsProcessed = 0;
				
		try {
			while (this.iterator.hasNext()) {
				totalQuadsProcessed++;				
				Object2Quad stmt = new Object2Quad(this.iterator.next());
				sniffer.sniff(stmt.getStatement());
				
				if (lstMetricConsumers != null){
					for (MetricProcess mConsumer : lstMetricConsumers) {
						// Notify each metric process about the new triple. Note that if the queue of triples to process of any metric process 
						// gets full, the call to notifyNewQuad() will block until space becomes available in that queue. This will in fact, 
						// block the triple distribution altogether (for all other metric processes too)
						try {
							mConsumer.notifyNewQuad(stmt);
						} catch(InterruptedException iex) {
							logger.warn("/!\\ Quad lost for {}!!!. Interrumpted thread while notifying quad #: {} to metric: {}. Error: {}", 
									datasetURI, totalQuadsProcessed, mConsumer.metricName, iex.getMessage());
						}
					}
				}
			}
		} catch(RiotException rex) {
			logger.warn("Failed to process dataset: {}. RIOT Exception while attempting to process quad # : {}. Error details: {}", 
					datasetURI, totalQuadsProcessed, rex.getMessage());
			throw new RuntimeException(rex);
		} catch(Exception ex) {
			logger.error("Failed to process dataset: {}. Exception while attempting to process quad # : {}. Error details: {}", 
					datasetURI, totalQuadsProcessed, ex);
			throw new RuntimeException(ex);
		}
		finally {
			if (lstMetricConsumers != null) {
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
			}		
		}
		
		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
		}
		
		for(String clazz : metricInstances.keySet()) {
			if(metricInstances.get(clazz) instanceof ComplexQualityMetric) {
				try {
					List<Args> args = loader.getBeforeArgs(clazz);
					
					List<Object> pass = new ArrayList<Object>();
					for(Args arg : args) {
						pass.add(this.transformJavaArgs(Class.forName(arg.getType()), arg.getParameter()));
					}

					((ComplexQualityMetric)this.metricInstances.get(clazz)).after(pass.toArray());				
				} catch (AfterException | ClassNotFoundException  e) {
					logger.error(e.getMessage());
				}
			}
//			metricInstances.get(clazz).metricValue();
		}
	}
	
	private void loadMetrics() throws ExternalMetricLoaderException {
		NodeIterator iter = metricConfiguration.listObjectsOfProperty(LMI.metric);
		Map<String, Class<? extends QualityMetric>> map = loader.getQualityMetricClasses();
		
		try {
			map.putAll(this.dmc.compile());
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		while(iter.hasNext()){
			String className = iter.next().toString();
			Class<? extends QualityMetric> clazz = map.get(className);
			QualityMetric metric = null;
			try {
				metric = clazz.newInstance();
				metric.setDatasetURI(this.baseURI);
			} catch (InstantiationException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance for " + className + ". Exception caused by an Instantiation Exception : " + e.getLocalizedMessage());
			} catch (IllegalAccessException e) {
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance " + className + ". Exception caused by an Illegal Access Exception : " + e.getLocalizedMessage());
			} catch (NullPointerException e){
				logger.error("Cannot load metric for {}", className);
				throw new ExternalMetricLoaderException("Cannot create class instance " + className + ". Exception caused by an Illegal Access Exception : " + e.getLocalizedMessage());
			}
			metricInstances.put(className, metric);
		}
		
		for(String className : this.metricInstances.keySet()) {
			if (this.metricInstances.get(className) instanceof ComplexQualityMetric){
				try {
					List<Args> args = loader.getBeforeArgs(className);
				
					List<Object> pass = new ArrayList<Object>();
					for(Args arg : args){
						pass.add(this.transformJavaArgs(Class.forName(arg.getType()), arg.getParameter()));
					}

					((ComplexQualityMetric)this.metricInstances.get(className)).before(pass.toArray());
				} catch (BeforeException | ClassNotFoundException  e) {
					logger.error(e.getMessage());
				}
			}
			this.lstMetricConsumers.add(new MetricProcess(this.metricInstances.get(className)));
		}
		
	}
	
	private Object transformJavaArgs(Class<?> target, String s){
		 if (target == Object.class || target == String.class || s == null) {
		        return s;
		    }
		    if (target == Character.class || target == char.class) {
		        return s.charAt(0);
		    }
		    if (target == Byte.class || target == byte.class) {
		        return Byte.parseByte(s);
		    }
		    if (target == Short.class || target == short.class) {
		        return Short.parseShort(s);
		    }
		    if (target == Integer.class || target == int.class) {
		        return Integer.parseInt(s);
		    }
		    if (target == Long.class || target == long.class) {
		        return Long.parseLong(s);
		    }
		    if (target == Float.class || target == float.class) {
		        return Float.parseFloat(s);
		    }
		    if (target == Double.class || target == double.class) {
		        return Double.parseDouble(s);
		    }
		    if (target == Boolean.class || target == boolean.class) {
		        return Boolean.parseBoolean(s);
		    }
		    throw new IllegalArgumentException("Don't know how to convert to " + target);
	}

	/**
	 * Generates the quality report associated to this quality assessment process. 
	 * Sets the result into the qualityReport attribute
	 */
	private void generateQualityReport() {
		this.isGeneratingQR = true;
		QualityReport r = new QualityReport();
		List<String> qualityProblems = new ArrayList<String>();
		
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			ProblemList<?> qProbs = m.getQualityProblems();
			if (qProbs == null) continue;
			qualityProblems.add(r.createQualityProblem(m.getMetricURI(), qProbs));
		}
		
		Resource res = ModelFactory.createDefaultModel().createResource(EnvironmentProperties.getInstance().getBaseURI());
		this.qualityReport = r.createQualityReport(res, qualityProblems);
		r.flush();
		this.isGeneratingQR = false;
		this.endedGeneratingQR = true;
	}
	
	/**
	 * Prints the quality meta-data produced by this quality assessment process onto the standard output 
	 */
	@SuppressWarnings("unused")
	private void generateQualityMetadata(){
		Resource res = ModelFactory.createDefaultModel().createResource(EnvironmentProperties.getInstance().getBaseURI());
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

	/**
	 * Generates the quality metadata corresponding to the data processed by this instance. Stores the 
	 * resulting metadata into a file, along the corresponding configuration parameters.
	 * TODO: Consider other concurrency cases such as: several instances of the JVM and different class loaders
	 */
	private synchronized void writeQualityMetadataFile() {
		this.isGeneratingQMD = true;
		// Build the full path of the file where quality metadata will be written
		String fld = this.metadataBaseDir + "/" + this.baseURI.replace("http://", "");
		fld = fld.replaceFirst("^~",System.getProperty("user.home"));
				
		File folder = new File(fld);
		folder.mkdirs();
		
		String metadataFilePath = fld + "/quality-meta-data.trig";
		metadataFilePath = metadataFilePath.replace("//", "/");
		logger.debug("Writing quality meta-data to file: metadataFilePath...");
				
		File fileMetadata = new File(metadataFilePath);
		Dataset model = DatasetFactory.createMem();
		// Verify whether there's already a quality metadata file for the assessed resource and load it if so
		if(fileMetadata.exists()) {
			RDFDataMgr.read(model, metadataFilePath, this.baseURI, Lang.TRIG);
		}
		
		// Note that createResource() intelligently reuses the resource if found within a read model
		Resource res = ModelFactory.createDefaultModel().createResource(this.baseURI);
		QualityMetadata md = new QualityMetadata(model, res);
		
		// Write quality metadata about the metrics assessed through this processor
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			md.addMetricData(m);
		}

		try {
			// Make sure the file is created (the following call has no effect if the file exists)
			fileMetadata.createNewFile();
			// Write new quality metadata into file
			OutputStream out = new FileOutputStream(fileMetadata, false);
			RDFDataMgr.write(out, md.createQualityMetadata(), RDFFormat.TRIG);
			
			logger.debug("Quality meta-data successfully written.");
		} catch(MetadataException | IOException ex) {
			logger.error("Quality meta-data could not be written to file: " + metadataFilePath, ex);
		}
		this.isGeneratingQMD = false;
		this.endedGeneratingQMD = true;
	}
	
	/**
	 * Writes the quality report generated as part of the this assessment process. Stores the 
	 * report metadata into a new file, identified by the along the corresponding configuration parameters.
	 * TODO: Consider other concurrency cases such as: several instances of the JVM and different class loaders
	 */
	private synchronized void writeReportMetadataFile() {
		// Build the full path of the file where quality report metadata will be written.
		// Use current timestamp to identify the report corresponding to each individual quality assessment process
		String fld = this.metadataBaseDir + "/" + this.baseURI.replace("http://", "");
		fld = fld.replaceFirst("^~",System.getProperty("user.home"));
				
		File folder = new File(fld);
		if (!(folder.exists())) folder.mkdirs();
		
		long timestamp = (new Date()).getTime();
		String metadataFilePath = String.format("%s/%s/problem-report-%d.trig", this.metadataBaseDir, this.baseURI.replace("http://", ""), timestamp);
		metadataFilePath = metadataFilePath.replace("//", "/");
		metadataFilePath = metadataFilePath.replaceFirst("^~",System.getProperty("user.home"));

		logger.debug("Writing quality report to file: metadataFilePath...");

		// Make sure that the quality report model has been properly generated before hand
		if(this.retreiveQualityReport() != null) {
			File fileMetadata = new File(metadataFilePath);
			Dataset model = DatasetFactory.create(this.retreiveQualityReport());
	
			try {
				// Make sure the file is created (the following call has no effect if the file exists)
				fileMetadata.createNewFile();
				// Write new quality metadata into file
				OutputStream out = new FileOutputStream(fileMetadata, false);
				RDFDataMgr.write(out, model, RDFFormat.TRIG);
				
				logger.debug("Quality report successfully written.");
			} catch(IOException ex) {
				logger.error("Quality meta-data could not be written to file: " + metadataFilePath, ex);
			}
		} else {
			logger.warn("Attempted to write quality report, but no report model has been generated");
		}
	}

	public Model retreiveQualityReport(){
		return this.qualityReport;
	}

	private final class MetricProcess {
		volatile BlockingQueue<Object2Quad> quadsToProcess = new ArrayBlockingQueue<Object2Quad>(500000);
		Thread metricThread = null;
		String metricName = null;
        
        Long stmtsProcessed = 0l;
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

		public void notifyNewQuad(Object2Quad newQuad) throws InterruptedException {
			// Try to set the incooming triple into the blocking queue, so that if its full, this thread blokcs (i.e. waits) until space is available in the queue
			quadsToProcess.put(newQuad);
			logger.trace("Metric {}, element put into queue (to-process: {})", this.metricName, quadsToProcess.size());
		}
		
		public void stop() {
			while(!quadsToProcess.isEmpty()) {
				logger.trace("Waiting for items on queue: {} Metric: {}", quadsToProcess.size(), this.metricName);
			}
			
			this.stopSignal = true;
		}
		
		public Long getStatementProcessed(){
			return this.stmtsProcessed;
		}
		
		public String getMetricName(){
			return this.metricName;
		}
		
		public void closeAssessment(){
			this.stopSignal = true;
			this.quadsToProcess.clear();
		}
		
		public boolean isDoneParsing(){
			return quadsToProcess.isEmpty();
		}

    }

	@Override
	public synchronized List<IOStats> getIOStats() throws ProcessorNotInitialised {
		
		List<IOStats> lst = new ArrayList<IOStats>();
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		
		for (MetricProcess mp : lstMetricConsumers){
			Long stmtProcessed = mp.getStatementProcessed();
			String metricName = mp.getMetricName();
			boolean doneParsing = mp.isDoneParsing();
			
			IOStats ios = new IOStats(metricName,stmtProcessed, doneParsing);
			
			if ((this.isGeneratingQMD) && (!this.endedGeneratingQMD))
				ios.setQmdStatus("Generating Quality Metadata");
			if ((!this.isGeneratingQMD) && (this.endedGeneratingQMD))
				ios.setQmdStatus("Finished Generating Quality Metadata");
			
			if ((this.isGeneratingQR) && (!this.endedGeneratingQR))
				ios.setQmdStatus("Generating Quality Problem Report");
			if ((!this.isGeneratingQR) && (this.endedGeneratingQR))
				ios.setQmdStatus("Finished Generating Quality Problem Report");
			
			lst.add(ios);
		}
		
		return lst;
	}
	
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	

		forcedCancel = true;
		
		for (MetricProcess mp : lstMetricConsumers){
			logger.info("Closing and clearing quads queue for {}", mp.metricName);
			mp.closeAssessment();
		}
		
		logger.info("Closing Iterators");
		try{
			this.iterator.close();
			this.rdfStream.finish();
		}catch (RiotException re){
			logger.info("RDF Stream already closed");
		}

		executor.shutdownNow();
	}
}
