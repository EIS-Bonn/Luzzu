package de.unibonn.iai.eis.luzzu.communications.resources;

import java.util.List;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDataset.Quad;
import com.github.jsonldjava.utils.JsonUtils;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import de.unibonn.iai.eis.luzzu.io.impl.StreamProcessor;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;

/**
 * REST resource, providing the functionalities to assess the quality of datasets, 
 * with respect to a set of metrics of interest
 * @author slondono
 */
@Path("/")
public class QualityResource {
	
	final static Logger logger = LoggerFactory.getLogger(QualityResource.class);

	/**
	 * Initiates the calculation of a specificed set of quality metrics on the dataset with the provided URI, 
	 * returns as response, a report listing the triple instances violating quality metrics
	 * @param formParams parameters for the calculation: Dataset = URI of the dataset to evaluate, 
	 * 			QualityReportRequired = boolean, should a quality report be generated?, 
	 * 			MetricsConfiguration = JSON-LD specifying the metrics to be calculated (refer to documentation for details)
	 * @return quality report in JSON-LD format, if parameter QualityReportRequired was true, 
	 * 			otherwise, the dataset URI (refer to documentation for details)
	 */
	@POST
	@Path("compute_quality")
	@Produces(MediaType.APPLICATION_JSON)
	public String computeQuality(MultivaluedMap<String, String> formParams) {
		
		StringBuilder sbJsonOutput = new StringBuilder();
		
		try {
			logger.info("Quality computation request received for dataset: {}", formParams.get("Dataset"));
			
			// Extract and validate parameters
			List<String> lstDatasetURI = formParams.get("Dataset");
			List<String> lstQualityReportReq = formParams.get("QualityReportRequired");
			List<String> lstMetricsConfig = formParams.get("MetricsConfiguration");
			
			logger.debug("Processing request parameters. DatasetURI: {}; QualityReportRequired: {}; MetricsConfiguration: {}", 
					lstDatasetURI, lstQualityReportReq, lstMetricsConfig);
									
			if(lstDatasetURI == null || lstDatasetURI.size() <= 0) {
				throw new IllegalArgumentException("Dataset URI parameter was not provided");
			}
			if(lstQualityReportReq == null || lstQualityReportReq.size() <= 0) {
				throw new IllegalArgumentException("Generate Quality Report parameter was not provided");
			}
			if(lstMetricsConfig == null || lstMetricsConfig.size() <= 0) {
				throw new IllegalArgumentException("Metrics configuration parameter was not provided");
			}
			
			// Assign parameter values to variables and set defaults
			String datasetURI = lstDatasetURI.get(0);
			String jsonStrMetricsConfig = lstMetricsConfig.get(0);
			boolean genQualityReport = Boolean.parseBoolean(lstQualityReportReq.get(0));

			// Parse the metrics configuration as JSON-LD and convert it to RDF to extract its triples
			Object jsonObj = JsonUtils.fromString(jsonStrMetricsConfig);
			logger.debug("Parsing metrics config as JSON-LD string. Resulting object: ", (jsonObj != null)?(jsonObj.getClass().getName()):("null"));
			
			RDFDataset rdf = (RDFDataset)JsonLdProcessor.toRDF(jsonObj, new JsonLdOptions());	
			List<Quad> lst = rdf.getQuads("@default");
			
			Model modelConfig = ModelFactory.createDefaultModel();
			Resource rSubj = Commons.generateURI();
			
			// Build a Jena model representing the metrics configuration, as expected by the StreamProcessor
			for(Quad qd : lst) {
				Property prop = modelConfig.createProperty(qd.getPredicate().getValue());
				Literal litVal = modelConfig.createLiteral(qd.getObject().getValue());
				modelConfig.add(rSubj, prop, litVal);
				logger.trace("Added statement to metrics config model: {}, {}, {}", rSubj, prop, litVal);
			}

			StreamProcessor strmProc = new StreamProcessor(datasetURI, genQualityReport, modelConfig);
			strmProc.cleanUp();
			// strmProc.startProcessing();
			
			// Append dataset URI to the output
			sbJsonOutput.append("{ \"Dataset\": \"" + datasetURI + "\", ");
			
			if(genQualityReport) {
				// TODO: Build quality graph and serialize as JSON-LD
				sbJsonOutput.append("\"QualityReport\": {}");
			}

			sbJsonOutput.append(" }");
			logger.debug("Quality computation request completed. Output: {}", sbJsonOutput.toString());
			
		} catch(Exception ex) {
			// TODO: Build appropriate output on error
			logger.error("Error processing quality computation request", ex);
		}
		
		return sbJsonOutput.toString();
	}
	
	public static void main (String [] args){
		MultivaluedMap<String, String> m = new MultivaluedHashMap<String, String>();
		m.add("Dataset", "http://oeg-dev.dia.fi.upm.es/licensius/rdflicense/rdflicense.ttl");
		m.add("QualityReportRequired", "false");
		m.add("MetricsConfiguration", "{\"@id\": \"_:f4231157584b1\",\"@type\": [\"http://github.com/EIS-Bonn/Luzzu#LuzzuMetricJavaImplementation\"],\"http://github.com/EIS-Bonn/Luzzu#metric\": [{\"@value\": \"diachron.metrics.contextual.amountofdata.AmountOfTriples\"}]}");
		
		QualityResource q = new QualityResource();
		q.computeQuality(m);
	}

}
