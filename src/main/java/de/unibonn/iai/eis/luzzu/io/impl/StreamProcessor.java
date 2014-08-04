package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
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
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;

public class StreamProcessor implements IOProcessor {

	//http://jena.apache.org/documentation/io/rdf-input.html

	protected String datasetURI; // this variable holds the filename or uri of datasets which needs to be processed
	private PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	
	private ExecutorService executor = Executors.newSingleThreadExecutor(); // PipedRDFStream and PipedRDFIterator need to be on different threads

	private boolean isInitalised = false;
	
	public StreamProcessor(String datasetURI){
		this.datasetURI = datasetURI;
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
		
		
		// load Usecase specific metrics -- added for review year 1 prototype TODO: modify to be generic
		try {
			this.loadUsecaseSpecificMetrics("ebi");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public void startProcessing() throws ProcessorNotInitialised{
		//TODO: if setUp is not called, throw an error
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");
		
		Runnable parser = new Runnable(){
			public void run() {
				RDFDataMgr.parse(rdfStream, datasetURI);
			}
		};
		
		executor.submit(parser);  // Start the parser on another thread
	

		// loop which will go through the statements one by one
		while (this.iterator.hasNext()){
			Object2Quad stmt = new Object2Quad(this.iterator.next());
			
			for(String className : this.metricInstances.keySet()){
				QualityMetric m = this.metricInstances.get(className);
				m.compute(stmt.getStatement());
			}
		}
		this.generateQualityGraph();
	}


	public void cleanUp() throws ProcessorNotInitialised{
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");
		
		if (!this.executor.isShutdown()){
			this.executor.shutdown();
		}
	}
	
	///// TESTING /////
	private Map<String, QualityMetric> metricInstances = new ConcurrentHashMap<String, QualityMetric>();
	
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
	
	public void generateQualityGraph(){
		HashMap<String,Resource> generatedURI = new HashMap<String,Resource>();
		
		Model m = ModelFactory.createDefaultModel();
		
			for(String className : this.metricInstances.keySet()){
				QualityMetric met = this.metricInstances.get(className);
				//System.out.println(met.getMetricURI());
				List<Statement> daqTrips = met.toDAQTriples();
				m.add(daqTrips);
				
				Resource dim = DAQHelper.getDimensionResource(met.getMetricURI());
				if (!(generatedURI.containsKey(dim.getURI()))){
					Resource dimURI = Commons.generateURI();
					generatedURI.put(dim.getURI(), dimURI);
				}
				Statement dtype = new StatementImpl(generatedURI.get(dim.getURI()), RDF.type, dim);
				Property p = m.createProperty(DAQHelper.getPropertyResource(met.getMetricURI()).getURI());
				Statement dhasMet = new StatementImpl(generatedURI.get(dim.getURI()), p , daqTrips.get(0).getSubject());
				m.add(dtype);
				m.add(dhasMet);
				
				Resource cat = DAQHelper.getCategoryResource(met.getMetricURI());
				if (!(generatedURI.containsKey(cat.getURI()))){
					Resource catURI = Commons.generateURI();
					generatedURI.put(cat.getURI(), catURI);
				}
				
				Statement ctype = new StatementImpl(generatedURI.get(cat.getURI()), RDF.type, cat);
				p = m.createProperty(DAQHelper.getPropertyResource(dim).getURI());
				Statement chasDim = new StatementImpl(generatedURI.get(cat.getURI()), p, daqTrips.get(0).getSubject());
				m.add(ctype);
				m.add(chasDim);
			}
		
		try {
			String filename = "/Users/jeremy/Documents/Workspaces/eis/quality/src/main/resources/output/qg.trig";
			RDFDataMgr.write(new FileOutputStream(filename,true), m, Lang.TRIG);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws ProcessorNotInitialised, ClassNotFoundException, InstantiationException, IllegalAccessException{
		
		for (int i=34; i<=44; i++){
			if (i == 40) i++;
			String filename = StreamProcessor.class.getClassLoader().getResource("testfiles/ebi/efo-2."+i+".rdf").toExternalForm();
			
			System.out.println("Processing efo-2."+i+".rdf");
			process(filename);
		}
	}
	
	public static void process(String filename) throws ProcessorNotInitialised, ClassNotFoundException, InstantiationException, IllegalAccessException{
		StreamProcessor sp = new StreamProcessor(filename);
		sp.setUpProcess();
		try {
			sp.startProcessing();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			sp.cleanUp();
		}
		System.out.println("finished");
	}
	
	
	
//	public static void main(String[] args) throws ProcessorNotInitialised{
//		String uri = "http://aksw.org/model/export/?m=http%3A%2F%2Faksw.org%2F&f=rdfxml";
//		String filename = "D:\\Users\\jdebattist\\Desktop\\raw_infobox_properties_en.nq";
//		
//		StreamProcessor sp = new StreamProcessor(uri);
//		sp.setUpProcess();
//		sp.startProcessing();
//		sp.cleanUp();
//	}
}
