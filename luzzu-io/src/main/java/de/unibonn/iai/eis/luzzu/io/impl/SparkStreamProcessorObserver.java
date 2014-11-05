package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.io.Serializable;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import de.unibonn.iai.eis.luzzu.annotations.QualityMetadata;
import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.assessment.ComplexQualityMetric;
import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.ExternalMetricLoaderException;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.DeclerativeMetricCompiler;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.qml.parser.ParseException;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;

/**
 * @author Jeremy Debattista
 *
 *	http://jena.apache.org/documentation/io/rdf-input.html
 */
public class SparkStreamProcessorObserver  implements IOProcessor, Serializable  {
	
	private static final long serialVersionUID = 2448767269028661064L;

	//	private final CacheManager cacheMgr = CacheManager.getInstance();
//	private final String graphCacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	transient private ConcurrentMap<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	transient private ExternalMetricLoader loader = ExternalMetricLoader.getInstance();
	transient private DeclerativeMetricCompiler dmc  = DeclerativeMetricCompiler.getInstance();

	transient final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);

	private String datasetURI;
	private boolean genQualityReport;
	transient private Model metricConfiguration;
	transient private Model qualityReport;
	
	transient private ExecutorService executor; // PipedRDFStream and PipedRDFIterator need to be on different threads
	
	private JavaRDD<String> datasetRDD; 
	
	private boolean isInitalised = false;
			
	public SparkStreamProcessorObserver(String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetURI = datasetURI;
		this.datasetRDD = sc.textFile(datasetURI);
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
//		cacheMgr.createNewCache(graphCacheName, 50);
		
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
		} else {
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
	}

	public void cleanUp() throws ProcessorNotInitialised{
		
		this.isInitalised = false;
		
		// Clear list of metrics
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
				
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
	}
	
	private static SparkConf conf = new SparkConf().setAppName("Luzzu").setMaster("spark://146.148.11.180:7077"); // TODO: fix appname and master
	private static JavaSparkContext sc = new JavaSparkContext(conf);
	private static  List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();
	private static StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
	
	
	//rabbitmq
	private final static String QUEUE_NAME = "luzzu";
	private static Connection connection;
	private static Channel channel;
	static{
		 	ConnectionFactory factory = new ConnectionFactory();
		    factory.setHost("jerdeb:qwerty12@130.211.112.37");
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void startProcessing() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
				
		JavaRDD<String> queue = datasetRDD.map(new Function<String, String>(){
			private static final long serialVersionUID = -44291655703031316L;

			public String call(String quadOrTriple){
				return quadOrTriple;
			}
		});
		
		try {
			queue.foreach(new VoidFunction<String>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 5371361721653134126L;

				public void call(String a) {
//					Triple t = toTripleStmt(a);
//					Object2Quad stmt = new Object2Quad(t);
					try {
						channel.queueDeclare(QUEUE_NAME, false, false, false, null);
					    channel.basicPublish("", QUEUE_NAME, null, a.getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
//					sniffer.sniff(stmt.getStatement());
//					for(MetricProcess mConsumer : lstMetricConsumers) {
//						mConsumer.notifyNewQuad(stmt);
//					}
				}});
		}	finally {
			try {
				channel.close();
			    connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (lstMetricConsumers != null){
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}	
			}		
		}
		
//		if (sniffer.getCachingObject() != null) {
//			cacheMgr.addToCache(graphCacheName, datasetURI, sniffer.getCachingObject());
//		}
		
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
		volatile Queue<Object2Quad> quadsToProcess = new BlockingArrayQueue<Object2Quad>(1000000);
		Thread metricThread = null;
		String metricName = null;
        
        Integer stmtsProcessed = 0;
        boolean stopSignal = false;
        
        private final static String QUEUE_NAME = "hello";
        private Connection connection;
    	private Channel channel;
        
        MetricProcess(final QualityMetric m) { 
        	
        	ConnectionFactory factory = new ConnectionFactory();
		    factory.setHost("jerdeb:qwerty12@130.211.112.37");
			try {
				connection = factory.newConnection();
				channel = connection.createChannel();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
        	this.metricName = m.getClass().getSimpleName();
        	        	
        	this.metricThread = (new Thread() { 
        		@Override
        		public void run() {
        			logger.debug("Starting thread for metric {}", m.getClass().getName());
        			
        			Object2Quad curQuad = null;
        			
        			QueueingConsumer consumer = new QueueingConsumer(channel);
        		    try {
						channel.basicConsume(QUEUE_NAME, true, consumer);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        			
        		    QueueingConsumer.Delivery delivery = null;
        			try {
						while(!stopSignal || ((delivery = consumer.nextDelivery()) != null)) {
							
							curQuad = new Object2Quad(toTripleStmt(new String(delivery.getBody())));
							
							if(curQuad != null) {
								logger.trace("Metric {}, new quad (processed: {}, to-process: {})", m.getClass().getName(), stmtsProcessed, quadsToProcess.size());
								
								m.compute(curQuad.getStatement());
								
								curQuad = null;
								stmtsProcessed++;
							}
						}
					} catch (ShutdownSignalException
							| ConsumerCancelledException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        			System.out.println(stmtsProcessed);
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
			
			// Wait until finished
			while(!quadsToProcess.isEmpty()) {
				// Just wait
				logger.debug("Waiting for items on queue: {} Metric: {}", quadsToProcess.size(), this.metricName);
			}
			
			this.stopSignal = true;
		}

    }
	
	private static Model m = ModelFactory.createDefaultModel();
	
	private static Triple toTripleStmt(String stmt){
		//TODO: this is a very quick hack and it is only for the experimentation phase
		Triple t = null;
		
		String[] triples = stmt.split(" "); //assuming that s p o are separated by a space
		
		//subject
		String subject = triples[0].replace("<", "").replace(">", "");
		Resource _s = m.createResource(subject);
		
		String predicate = triples[1].replace("<", "").replace(">", "");
		Property _p = m.createProperty(predicate);
		
		String object = triples[2];
		if (object.startsWith("<") && object.endsWith(">")){
			// this is a resource
			object = object.replace("<", "").replace(">", "");
			Resource _o = m.createResource(object);
			t = new Triple(_s.asNode(), _p.asNode(), _o.asNode());
		} else {
			// object is a literal
			object = object.replace("\"", "");
			Literal _o = m.createLiteral(object);
			t = new Triple(_s.asNode(), _p.asNode(), _o.asNode());
		}
		
		return t;
	}

}
