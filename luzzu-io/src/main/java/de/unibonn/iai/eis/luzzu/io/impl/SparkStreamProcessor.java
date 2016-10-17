package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import de.unibonn.iai.eis.luzzu.annotations.QualityMetadata;
import de.unibonn.iai.eis.luzzu.annotations.QualityReport;
import de.unibonn.iai.eis.luzzu.assessment.ComplexQualityMetric;
import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.datatypes.Args;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.AfterException;
import de.unibonn.iai.eis.luzzu.exceptions.BeforeException;
import de.unibonn.iai.eis.luzzu.exceptions.ExternalMetricLoaderException;
import de.unibonn.iai.eis.luzzu.exceptions.MetadataException;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.configuration.DeclerativeMetricCompiler;
import de.unibonn.iai.eis.luzzu.io.configuration.ExternalMetricLoader;
import de.unibonn.iai.eis.luzzu.io.helper.IOStats;
import de.unibonn.iai.eis.luzzu.io.helper.TriplePublisher;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.qml.parser.ParseException;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.LMI;


public class SparkStreamProcessor  implements IOProcessor, Serializable  {
	
	private static final long serialVersionUID = 2448767269028661064L;
	transient private static final Properties sparkProperties = PropertyManager.getInstance().getProperties("spark.properties");

	transient private ConcurrentMap<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	transient private ExternalMetricLoader loader = null;//ExternalMetricLoader.getInstance();
	transient private DeclerativeMetricCompiler dmc  = null;//DeclerativeMetricCompiler.getInstance();

	transient final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);

	private String datasetURI;
	private boolean genQualityReport;
	transient private Model metricConfiguration;
	transient private Model qualityReport;
	
	transient private ExecutorService executor; 
	
	private JavaRDD<String> datasetRDD;
		
	private boolean isInitalised = false;
	
	private TriplePublisher triplePublisher = new TriplePublisher();
	private List<MetricProcess> lstMetricConsumers = new ArrayList<MetricProcess>();
			
	public SparkStreamProcessor(String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetURI = datasetURI;
//		
//		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://192.168.99.100:9000");
//		FileSystem fs = null;
		File f = new File(datasetURI);;
//		try {
//			fs = FileSystem.get(conf);
//			f = new File(datasetURI); //f.getCanonicalPath()
//			Path localPath = new Path(f.toURI());
//			Path hdfsPath = new Path("/data/"+f.getName());
//			fs.copyFromLocalFile(localPath, hdfsPath);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		


		
		this.datasetRDD = sc.textFile("hdfs://192.168.99.100:9000/data/"+f.getName());
		this.genQualityReport = genQualityReport;
		this.metricConfiguration = configuration;
		
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
		
//		try {
//			this.loadMetrics();
//		} catch (ExternalMetricLoaderException e) {
//			logger.error(e.getLocalizedMessage());
//		}
		
		this.executor = Executors.newSingleThreadExecutor();
	}

	public void cleanUp() throws ProcessorNotInitialised{
		
		this.isInitalised = false;
				
		// Clear list of metrics
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
		
		// Close triple publisher
		this.triplePublisher.close();
				
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}
	}
	
	public static void close() {
		if(connection.isOpen()) {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	//private static SparkConf conf = new SparkConf().setMaster(sparkProperties.getProperty("SPARK_SERVER")).setAppName(sparkProperties.getProperty("APPLICATION_NAME"));
	private static JavaSparkContext sc = new JavaSparkContext("spark://192.168.99.100:7077", "Luzzu");	
	
    private static Connection connection;
    static {
    	ConnectionFactory factory = new ConnectionFactory();
    	factory.setHost(sparkProperties.getProperty("RABBIT_MQ_SERVER"));
        factory.setUsername(sparkProperties.getProperty("RABBIT_MQ_USERNAME"));
        factory.setPassword(sparkProperties.getProperty("RABBIT_MQ_PASSWORD"));
        factory.setVirtualHost(sparkProperties.getProperty("RABBIT_MQ_VIRTUALHOST"));
        factory.setPort(Integer.parseInt(sparkProperties.getProperty("RABBIT_MQ_PORT")));
        try {
			connection = factory.newConnection();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }
    

	public void startProcessing() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");		
		
		try {
			triplePublisher.publishTriples(datasetRDD);
		} catch(IOException e){
			logger.error("I/O error publising triples", e);
		} finally {		
			if (lstMetricConsumers != null){
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
				
				boolean allFinished = false;
				
				while(!allFinished) {
					allFinished = true;
					
					for(MetricProcess mConsumer : lstMetricConsumers) {
						allFinished = (allFinished & mConsumer.isFinished());
					}
					
					logger.trace("Not all finished...");
				}
				
				logger.debug("All finished!!!");
			}
		}
		
		
		for(String clazz : metricInstances.keySet()){
			if(metricInstances.get(clazz) instanceof ComplexQualityMetric){
				try {
					List<Args> args = loader.getBeforeArgs(clazz);
					
					List<Object> pass = new ArrayList<Object>();
					for(Args arg : args){
						pass.add(this.transformJavaArgs(Class.forName(arg.getType()), arg.getParameter()));
					}

					((ComplexQualityMetric)this.metricInstances.get(clazz)).after(pass.toArray());				
				} catch (AfterException | ClassNotFoundException  e) {
					logger.error(e.getMessage());
				}
			}
			metricInstances.get(clazz).metricValue();
		}
	}


	@SuppressWarnings("unused")
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
			
			this.lstMetricConsumers.add(new MetricProcess(this.metricInstances.get(className), connection));
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
	
	private void generateQualityReport() {
		QualityReport r = new QualityReport();
		List<String> qualityProblems = new ArrayList<String>();
		
		String datasetURI = "";
		for(String className : this.metricInstances.keySet()){
			QualityMetric m = this.metricInstances.get(className);
			qualityProblems.add(r.createQualityProblem(m.getMetricURI(), m.getQualityProblems()));
			datasetURI = m.getDatasetURI();
		}
		
		Resource res = ModelFactory.createDefaultModel().createResource(datasetURI);
		this.qualityReport = r.createQualityReport(res, qualityProblems);
		r.flush();
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
	
	private static Model m = ModelFactory.createDefaultModel();
	
	private class MetricProcess implements Serializable {
		private static final long serialVersionUID = -5844313472934426666L;
		
		Thread metricThread = null;
		String metricName = null;
	    
	    Integer stmtsProcessed = 0;
	    boolean stopSignal = false;
	    boolean hasFinished = false;
	    	    	    
	    private final static String EXCHANGE_NAME = "triples_publish";
	    
		private Channel channel;
		private QueueingConsumer consumer;
		private String subscribeQueueName;
		
	    MetricProcess(final QualityMetric m, final Connection connection) { 
			try {
				channel = connection.createChannel();
				channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
								
				subscribeQueueName = channel.queueDeclare().getQueue();
		        channel.queueBind(subscribeQueueName, EXCHANGE_NAME, "");
		        
		        consumer = new QueueingConsumer(channel);
		        channel.basicConsume(subscribeQueueName, true, consumer);
				
			} catch (IOException e) {
				logger.error("Error initializing subscriber", e);
			}
			
	    	this.metricName = m.getClass().getSimpleName();

	    	this.metricThread = (new Thread() { 
	    		@Override
	    		public void run() {
	    			logger.debug("Starting thread for metric {}", metricName);
	    			
	    			Object2Quad curQuad = null;
	    		    QueueingConsumer.Delivery delivery = null;
	    		    
	    			try {
						while(!stopSignal || (delivery = consumer.nextDelivery(250)) != null) {
																					
							if(delivery != null) {
								curQuad = new Object2Quad(toTripleStmt(new String(delivery.getBody())));
								
								if(curQuad != null) {
									logger.trace("Metric {}, new quad (processed: {})", m.getClass().getName(), stmtsProcessed);
									
									m.compute(curQuad.getStatement());
									curQuad = null;
									stmtsProcessed++;
								}
							} else {
								stmtsProcessed += 0;
								logger.trace("Metric {}, got null delivery", m.getClass().getName());
							}
						}
						
						hasFinished = true;
					} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
						logger.error("Error processing triple", e);
					} finally {
						try {
							if(channel.isOpen()) {
								channel.close();
							}
						} catch (IOException e) {
							logger.warn("Could not close channel or connection");
						}
					}
	    			
	    			System.out.println(stmtsProcessed);
	    			logger.debug("Thread for metric {} completed, total statements processed {}", m.getClass().getName(), stmtsProcessed);
	    		}
	    		
	    	});
	    	
	    	this.metricThread.start();
	    }
		
		public void stop() {
			this.stopSignal = true;
		}
		
		public boolean isFinished() {
			return this.hasFinished;
		}
		
	}
	
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

	@Override
	public synchronized List<IOStats> getIOStats() throws ProcessorNotInitialised {
		//TODO
		return null;
	}
	
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		//TODO
	}

}
