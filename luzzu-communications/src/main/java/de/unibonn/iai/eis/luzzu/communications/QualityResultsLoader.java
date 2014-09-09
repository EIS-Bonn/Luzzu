package de.unibonn.iai.eis.luzzu.communications;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.SDMXATTRIBUTE;

/**
 * A singleton to process trig files containing quality results, to complete the models they contain as required by CubeViz and to
 * upload the results automatically to OntoWiki so that they're made available to the Web Interface 
 * @author slondono
 */
public final class QualityResultsLoader {
	
	final static Logger logger = LoggerFactory.getLogger(QualityResultsLoader.class);
	
	private static final String ontowikiBaseURL;
	private static final String ontowikiLogin;
	private static final String ontowikiPassword;
	
	// Singleton instance, eagerly initialized
	private static QualityResultsLoader loaderInstance = new QualityResultsLoader();
	
	static {
		// TODO: should be loaded from configuration
		ontowikiBaseURL = "http://localhost/ontowiki/";
		ontowikiLogin = "dba";
		ontowikiPassword = "dba";
	}
	
	/**
	 * Constructor. Private as required by the singleton pattern
	 */
	private QualityResultsLoader() {
		
	}

	/**
	 * Returns the Quality Results loader instance, which is eagerly initialized (thus doesn't have to be synchronized)
	 * @return Instance of Quality Results loader
	 */
	public static QualityResultsLoader getInstance() {		
		return loaderInstance;
	}
	
	/**
	 * Reads all quality metadata files (results of running the quality assessment process), completes their Quality Graph models 
	 * and stores them into OntoWiki. Files are deleted after being successfully processed.
	 * TODO: consider running this process asynchronously and serve processing requests through some queueing mechanism
	 */
	public synchronized void processAllQualityResultFiles() {
		
		// Retrieve all trig files in the qualityMetadata directory, where quality assessment results are stored
		File dirQualityMetaData = new File("qualityMetadata/");
		File[] arrQualityTrigFiles = dirQualityMetaData.listFiles( 
				new FileFilter() {
					public boolean accept(File pathname) {
						// Process files in trig format only
						if(pathname.getName() != null && pathname.getName().endsWith(".trig")) {
							return true;
						}
						return false;
					} });
		
		// Read all trig files containing Quality Metadata and process them
		QualityLoadResult curQualityResults = null;
		
		for(File qualityTrigFile : arrQualityTrigFiles) {
			
			logger.debug("Processing quality metadata file {}", qualityTrigFile.getName());
			
			// Load the Quality Graph in a model..
			curQualityResults = this.readTrigQualityGraph(qualityTrigFile);
			
			if(curQualityResults != null && curQualityResults.qualityModel != null && curQualityResults.dataSetURI != null && !curQualityResults.dataSetURI.equals("")) {
				
				// serialize the model in ttl format...
				StringWriter swQgWriter = new StringWriter();
				curQualityResults.qualityModel.write(swQgWriter, "TURTLE");
				
				// ... and load it into the repository (OntoWiki)
				this.sendCreateResourceRequest(curQualityResults.qualityGraphURI, swQgWriter.toString());
			} else {
				logger.warn("Attempted to process invalid quality metadata file. Quality results or dataset URI could not be loaded");
			}

			// Delete the quality metadata
			qualityTrigFile.delete();
			logger.debug("Quality metadata file {} deleted", qualityTrigFile.getName());
		}
	}

	public QualityLoadResult readTrigQualityGraph(File trigQualityFile) {
		
		InputStream inStrmQualityGraph = null;
		
		try {
			// Read trig file, which in case of Quality Graphs contains two parts...
			inStrmQualityGraph = new FileInputStream(trigQualityFile);
			
			// Read both parts into a dataset: a collection of named graphs
			Dataset dsQualityData = DatasetFactory.createMem();
			RDFDataMgr.read(dsQualityData, inStrmQualityGraph, Lang.TRIG);
			
			// Part 1, the default graph, contains the Quality Graph declaration
			Model mQualityGraphDecl = dsQualityData.getDefaultModel();
			Resource qGraphResource = null;
			String qualityGraphURI = null;
			String dataSetURI = null;
			
			// find the subject of the statement declaring the Quality Graph and take it as resource
			ResIterator iterQGraphResources = mQualityGraphDecl.listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
			
			if(iterQGraphResources.hasNext()) {
				qGraphResource = iterQGraphResources.next();
				
				// having this the Quality Graph's resource 
				StmtIterator iterQGraphStmts = mQualityGraphDecl.listStatements(new SimpleSelector(qGraphResource, null, (RDFNode)null));

				// Part 2, take the named graph corresponding to the Quality Graph's URI 
				qualityGraphURI = qGraphResource.getURI();
				Model mQualityGraph = dsQualityData.getNamedModel(qualityGraphURI);
				
				// Complete the Quality Graph model as required by CubeViz and store it as Knowledgebase
				if(mQualityGraph != null) {
					
					// firstly, add all statements in the default graph (declaration of the Quality Graph)
					while(iterQGraphStmts.hasNext()) {
						mQualityGraph.add(iterQGraphStmts.next());
					}
					
					// then add the dataset and all other statements as required by CubeViz
					dataSetURI = appendCubeStatements(qGraphResource, mQualityGraph);
				}
				
				return new QualityLoadResult(mQualityGraph, qualityGraphURI, dataSetURI);
			}

		} catch (Exception e) {
			logger.error("Error reading Quality Graph: " + ((trigQualityFile != null)?(trigQualityFile.getName()):("-")), e);
		} finally {
			if(inStrmQualityGraph != null) {
				try {
					inStrmQualityGraph.close();
				} catch (IOException e) {
					// Nothing to do here...
					logger.warn("Error closing stream when reading trig file {}", ((trigQualityFile != null)?(trigQualityFile.getName()):("-")));
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Adds all the statements required by CubeViz to the model and returns the URI of the 
	 * resource whose quality was assessed
	 * @param qGraphResource Resource corresponding to the subject of the QualityGraph property 
	 * @param mQualityGraph Quality Graph where the statements required by CubeViz will be inserted
	 * @return URI of the dataset for which quality information is provided in the model
	 */
	private String appendCubeStatements(Resource qGraphResource, Model mQualityGraph) {
		
		Resource csComputedOn = mQualityGraph.createResource(DAQ.getURI() + "csComputedOn");
		Resource csMetric = mQualityGraph.createResource(DAQ.getURI() + "csMetric");
		Resource csValue = mQualityGraph.createResource(DAQ.getURI() + "csValue");
		Resource unitMeasure = mQualityGraph.createResource(SDMXATTRIBUTE.getURI() + "unitMeasure");
		String assessedResourceURL = null;
		
		mQualityGraph.add(qGraphResource, RDF.type, CUBE.DataSet);
				
		mQualityGraph.add(csComputedOn, RDF.type, CUBE.ComponentSpecification);
		mQualityGraph.add(csComputedOn, RDFS.label, "Component Specification of Computed On");
		mQualityGraph.add(csComputedOn, CUBE.dimension, DAQ.computedOn);
		mQualityGraph.add(csComputedOn, CUBE.order, "2", XSDDatatype.XSDinteger);
		
		mQualityGraph.add(csMetric, RDF.type, CUBE.ComponentSpecification);
		mQualityGraph.add(csMetric, RDFS.label, "Component Specification of Metric");
		mQualityGraph.add(csMetric, CUBE.dimension, DAQ.metric);
		
		mQualityGraph.add(csValue, RDF.type, CUBE.ComponentSpecification);
		mQualityGraph.add(csValue, RDFS.label, "Component Specification of Value");
		mQualityGraph.add(csValue, CUBE.measure, DAQ.value);
		
		mQualityGraph.add(unitMeasure, RDF.type, CUBE.AttributeProperty);
		mQualityGraph.add(unitMeasure, RDFS.label, "Unit Attribute");
		
		mQualityGraph.add(DAQ.dsd, RDF.type, CUBE.DataStructureDefinition);
		mQualityGraph.add(DAQ.dsd, CUBE.component, csComputedOn);
		mQualityGraph.add(DAQ.dsd, CUBE.component, csMetric);
		mQualityGraph.add(DAQ.dsd, CUBE.component, csValue);
		mQualityGraph.add(DAQ.dsd, CUBE.component, unitMeasure);
		
		// Get URL of the assessed resource
		NodeIterator iterAssessed = mQualityGraph.listObjectsOfProperty(DAQ.computedOn);
		
		if(iterAssessed.hasNext()) {
			
			// Set the assessed resource's URL as label of the Quality Graph 
			// (temporary solution whilst it is decided wherefrom this info should be obtained)
			RDFNode assessedResource = iterAssessed.next();
			if(assessedResource != null) {
				assessedResourceURL = assessedResource.toString();
				mQualityGraph.add(qGraphResource, RDFS.label, assessedResourceURL);
				mQualityGraph.add(qGraphResource, RDFS.comment, "The Experimental Factor Ontology (EFO) provides a systematic description of many experimental variables available in EBI databases, and for external projects such as the NHGRI GWAS catalogue. It combines parts of several biological ontologies, such as anatomy, disease and chemical compounds. The scope of EFO is to support the annotation, analysis and visualization of data handled by the EBI Functional Genomics Team.");
			}
		}
		
		return assessedResourceURL;
	}
	
	private void sendCreateResourceRequest(String dataSetURI, String rdfData) {
		
		CloseableHttpClient httpClient = HttpClients.createDefault();
		// Create local HTTP context and Cookie store to hold session info
        HttpClientContext localContext = HttpClientContext.create();
		CookieStore cookieStore = new BasicCookieStore();
        localContext.setCookieStore(cookieStore);
		
        try {
        	// First step: log into OntoWiki in order to establish a valid session
        	EntityBuilder entityLogin = EntityBuilder.create();
        	entityLogin.setContentType(ContentType.APPLICATION_FORM_URLENCODED);
        	entityLogin.setParameters(
        			new BasicNameValuePair("logintype", "locallogin"), 
        			new BasicNameValuePair("password", ontowikiLogin), 
        			new BasicNameValuePair("username", ontowikiPassword),
        			new BasicNameValuePair("redirect-uri", ""));
        	
        	// Build and send HTTP request, notice that it must be run over the context created above
        	HttpPost httpPostLogin = new HttpPost(ontowikiBaseURL + "application/login");
        	httpPostLogin.setEntity(entityLogin.build());        	
        	CloseableHttpResponse responseLogin = httpClient.execute(httpPostLogin, localContext);
            
            try {
            	// Process response to the login request
            	logger.debug("Processed login request: {}, got HTTP status: {}", dataSetURI, responseLogin.getStatusLine());
                HttpEntity resEntityLogin = responseLogin.getEntity();                
                EntityUtils.consume(resEntityLogin);
            } finally {
            	// Immediately close and discard the connection
            	responseLogin.close();
            }
        	
            // Second step: send a request to create a new Knowledge base, feed with the code provided in the paste parameter
            // the create request consists of a Multipart/form-data entity containing the appropriate parameters
            MultipartEntityBuilder entityCreate = MultipartEntityBuilder.create();
    		entityCreate.addTextBody("referer", "");
    		entityCreate.addTextBody("modelUri", dataSetURI);
    		entityCreate.addTextBody("activeForm", "paste");
    		entityCreate.addTextBody("location", dataSetURI);
    		entityCreate.addTextBody("filetype-upload", "auto");
    		// Make sure that filename="" is added to the source param, otherwise an error processing the data will occur
    		entityCreate.addBinaryBody("source", new byte[0], ContentType.APPLICATION_OCTET_STREAM, "");
    		entityCreate.addTextBody("filetype-paste", "ttl");
    		// The RDF to be set into the knowledgebase is provided in the paste parameter
    		entityCreate.addTextBody("paste", rdfData);

    		// Build and send the HTTP request, notice that it must be run over the same context as the login request
    		HttpPost httpPostCreate = new HttpPost(ontowikiBaseURL + "model/create");
    		httpPostCreate.setEntity(entityCreate.build());
            CloseableHttpResponse responseCreate = httpClient.execute(httpPostCreate, localContext);
            
            try {
            	logger.debug("Processed create knowledgebase request: {}, got HTTP status: {}", dataSetURI, responseCreate.getStatusLine());
                HttpEntity resEntity = responseCreate.getEntity();
                EntityUtils.consume(resEntity);
            } finally {
            	// Immediately close and discard the connection
                responseCreate.close();
            }
        } catch (ClientProtocolException e) {
        	logger.error("Protocol error processing create knowledgbase request: " + dataSetURI, e);
		} catch (IOException e) {
			logger.error("I/O Error processing create knowledgbase request: " + dataSetURI, e);
		} finally {
        	try {
				httpClient.close();
			} catch (IOException e) {
				// Nothing to do here...
				logger.warn("Error closing HTTP Client while creating knowledgebase {}", dataSetURI);
			}
        }
	}
	
	/**
	 * Inner class used just to contain the multi-valued results returned by the quality loading process 
	 * @author slondono
	 */
	private class QualityLoadResult {
		
		public QualityLoadResult(Model qualityModel, String qualityGraphURI, String dataSetURI) {
			this.qualityModel = qualityModel;
			this.qualityGraphURI = qualityGraphURI;
			this.dataSetURI = dataSetURI;
		}
		
		public Model qualityModel;
		public String qualityGraphURI;
		public String dataSetURI;
	}

	public static void main(String[] args) {

		QualityResultsLoader resultsLoader = QualityResultsLoader.getInstance();
		resultsLoader.processAllQualityResultFiles();
		
//		File f = new File("qualityMetadata/urna32ea733-08e0-4d61-aa60-1bb72d1fffc4.trig");
//		Model mQualityGraph = resultsLoader.readTrigQualityGraph(f);
//
//		StringWriter swQgWriter = new StringWriter();
//		mQualityGraph.write(swQgWriter, "TURTLE");
//
//		resultsLoader.sendCreateResourceRequest("urn:f75d38c9-6aa8-4dea-b159-99c776b2a590", swQgWriter.toString());
	}

}
