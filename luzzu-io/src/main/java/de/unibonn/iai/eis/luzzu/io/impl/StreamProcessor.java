package de.unibonn.iai.eis.luzzu.io.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.cache.CacheManager;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

/**
 * @author Jeremy Debattista
 *
 *	http://jena.apache.org/documentation/io/rdf-input.html
 */
public class StreamProcessor implements IOProcessor {
	
	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String graphCacheName = PropertyManager.getInstance().getProperties("cache.properties").getProperty("GRAPH_METADATA_CACHE");
	private Map<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();


	protected String datasetURI;
	private PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	
	private ExecutorService executor = Executors.newSingleThreadExecutor(); // PipedRDFStream and PipedRDFIterator need to be on different threads

	private boolean isInitalised = false;
	
	
	public StreamProcessor(String datasetURI, boolean genQualityReport, Model configuration){
		this.datasetURI = datasetURI;
		// TODO: Complete setup
		cacheMgr.createNewCache(graphCacheName, 50);
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
		
		
		// load Usecase specific metrics 
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
		
	///// TESTING /////
	@SuppressWarnings("unchecked")
	private void loadUsecaseSpecificMetrics(String pilot) throws ClassNotFoundException, InstantiationException, IllegalAccessException{	
		String filename = this.getClass().getClassLoader().getResource("metrics_initalisations/metrics.trig").toExternalForm();

		Model m = ModelFactory.createDefaultModel();
		m.read(filename, null);
		
		String classPrefix = "de.unibonn.iai.eis.luzzu.qualitymetrics.";
		Resource pilotResourceURI = m.createResource("http://www.diachron-fp7.eu/diachron#"+pilot);
		Property metricClass = m.createProperty("http://www.diachron-fp7.eu/diachron#metric");
				
		StmtIterator si = m.listStatements(pilotResourceURI, metricClass, (RDFNode) null);
		
		while(si.hasNext()){
			String className = si.next().getObject().toString();
			className = classPrefix.concat(className);
			
			Class<? extends QualityMetric> clazz = (Class<? extends QualityMetric>) Class.forName(className);
			QualityMetric metric = clazz.newInstance();
			metricInstances.put(className, metric);
		}	
	}
	
	
	
}
