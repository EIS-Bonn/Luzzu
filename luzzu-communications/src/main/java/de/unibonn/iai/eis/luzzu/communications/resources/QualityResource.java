package de.unibonn.iai.eis.luzzu.communications.resources;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.luzzu.communications.Main;
import de.unibonn.iai.eis.luzzu.io.IOProcessor;
import de.unibonn.iai.eis.luzzu.io.ProcessorController;
import de.unibonn.iai.eis.luzzu.io.impl.SPARQLEndPointProcessor;
import de.unibonn.iai.eis.luzzu.io.impl.StreamProcessor;
import de.unibonn.iai.eis.luzzu.properties.EnvironmentProperties;

/**
 * REST resource, providing the functionalities to assess the quality of datasets, 
 * with respect to a set of metrics of interest
 * @author slondono
 */
@Path("/")
public class QualityResource {
	
	final static Logger logger = LoggerFactory.getLogger(QualityResource.class);
	
	private boolean isFree = true;
	

	@GET
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status(){
		StringBuilder sbJsonResponse = new StringBuilder();
		sbJsonResponse.append("{ \"isFree\": \"" + isFree + "\", ");
		sbJsonResponse.append("\"Agent\": \"" + Main.BASE_URI + "\", ");
		if (!isFree){
			sbJsonResponse.append("\"Serving Data Dump\": \"" + EnvironmentProperties.getInstance().getDatasetURI() + "\", ");
			sbJsonResponse.append("\"Base URI\": \"" + EnvironmentProperties.getInstance().getBaseURI() + "\", ");
		}
		sbJsonResponse.append("\"Outcome\": \"SUCCESS\"");
		sbJsonResponse.append("}");
		
		System.out.println(sbJsonResponse.toString());
		
		return Response.ok(sbJsonResponse.toString(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
	
	/**
	 * Initiates the calculation of a specificed set of quality metrics on the dataset with the provided URI, 
	 * returns as response, a report listing the triple instances violating quality metrics
	 * @param formParams parameters for the calculation: Dataset = URI of the dataset to evaluate
	 *  (a URI should be encoded to avoid unsafe ASCII characters such as '&'), 
	 * 			QualityReportRequired = boolean, should a quality report be generated?, 
	 * 			MetricsConfiguration = JSON-LD specifying the metrics to be calculated (refer to documentation for details)
	 * 			BaseUri = (Optional) define a base URI to the dataset when dataset dumps are split into more than one file/location
	 * @return quality report in JSON-LD format, if parameter QualityReportRequired was true, 
	 * 			otherwise, the dataset URI (refer to documentation for details)
	 */
	@POST
	@Path("compute_quality")
	@Produces(MediaType.APPLICATION_JSON)
	public Response computeQuality(MultivaluedMap<String, String> formParams) {
		
		String jsonResponse = null;
		String datasetURI = null;
		String jsonStrMetricsConfig = null;
		String baseURI = "";
		boolean genQualityReport = false;
		
		try {
			logger.info("Quality computation request received for dataset: {}", formParams.get("Dataset"));
			
			// Extract and validate parameters
			List<String> lstDatasetURI = formParams.get("Dataset");
			List<String> lstQualityReportReq = formParams.get("QualityReportRequired");
			List<String> lstMetricsConfig = formParams.get("MetricsConfiguration");
			List<String> lstBaseUri = formParams.get("BaseUri");
			List<String> lstIsSparql = formParams.get("IsSparql");

			logger.debug("Processing request parameters. DatasetURI: {}; QualityReportRequired: {}; MetricsConfiguration: {}; BaseUri: {}; IsSPARQLEndPoint: {}", 
					lstDatasetURI, lstQualityReportReq, lstMetricsConfig, lstBaseUri);
			
			System.out.printf("Processing request parameters. DatasetURI: %s; QualityReportRequired: %s; MetricsConfiguration: %s; BaseUri: %s; IsSPARQLEndPoint: %s", 
					lstDatasetURI, lstQualityReportReq.get(0), lstMetricsConfig.get(0), lstBaseUri.get(0), lstIsSparql.get(0));
									
			if(lstDatasetURI == null || lstDatasetURI.size() <= 0) {
				throw new IllegalArgumentException("Dataset URI parameter was not provided");
			}
			if(lstQualityReportReq == null || lstQualityReportReq.size() <= 0) {
				throw new IllegalArgumentException("Generate Quality Report parameter was not provided");
			}
			if(lstMetricsConfig == null || lstMetricsConfig.size() <= 0) {
				throw new IllegalArgumentException("Metrics configuration parameter was not provided");
			}
			if(lstBaseUri == null || lstBaseUri.size() <= 0) {
				throw new IllegalArgumentException("Base URI parameter was not provided");
			}
			if(lstIsSparql == null || lstIsSparql.size() <= 0) {
				throw new IllegalArgumentException("IsSparql parameter was not provided");
			}
			
			// Assign parameter values to variables and set defaults
			jsonStrMetricsConfig = lstMetricsConfig.get(0);
			genQualityReport = Boolean.parseBoolean(lstQualityReportReq.get(0));

			// Parse the metrics configuration as JSON-LD and convert it to RDF to extract its triples, note that no base URI is expected
			Model modelConfig = ModelFactory.createDefaultModel();
			RDFDataMgr.read(modelConfig, new StringReader(jsonStrMetricsConfig), null, Lang.JSONLD);
			
			// Obtain the base URI of the resources, which is important as it will be used to name the quality metadata models
			if (lstBaseUri != null) {
				baseURI = lstBaseUri.get(0);
			}
			
			boolean isSPARQLEndpoint = Boolean.parseBoolean(lstIsSparql.get(0));
			
			IOProcessor strmProc = null;

			if (isSPARQLEndpoint){
				String[] expandedListDatasetURI = lstDatasetURI.get(0).split(",");
				datasetURI = expandedListDatasetURI[0];
				strmProc = new SPARQLEndPointProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
			} else {
				String[] expandedListDatasetURI = lstDatasetURI.get(0).split(",");
				if (expandedListDatasetURI.length == 1){
					datasetURI = expandedListDatasetURI[0];
					if ((datasetURI.startsWith("http://")) || (datasetURI.startsWith("ftp://"))){
						//if it is an http dataset, then we cannot really identify the actual size
						strmProc = new StreamProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
					}else {
						strmProc = ProcessorController.getInstance().decide(baseURI, datasetURI, genQualityReport, modelConfig);
						logger.debug("Chosen Processor: {}", strmProc.getClass().toString());
						System.out.println("Chosen Processor: "+strmProc.getClass().toString());
					}
					
				} else {
					//if we have a void file (e.g. void.ttl) we have to make sure that it is processed first
					List<String> datasetFiles = Arrays.asList(expandedListDatasetURI);
					List<String> voidFiles = new ArrayList<String>();
					for(String s : datasetFiles) if (s.startsWith("void.")) voidFiles.add(s);
					for (String v : voidFiles) datasetFiles.remove(v);
					datasetFiles.addAll(0, voidFiles);
					
					strmProc = new StreamProcessor(baseURI, datasetFiles, genQualityReport, modelConfig);
				}
			}

			isFree = false;
			strmProc.processorWorkFlow();
			strmProc.cleanUp();
			
			Model modelQualityRep = null;
			
			// Retrieve quality report, if requested to do so
			if(genQualityReport) {
				modelQualityRep = strmProc.retreiveQualityReport();
			}
			
			jsonResponse = buildJsonResponse((datasetURI == null) ? baseURI : datasetURI, modelQualityRep);
			logger.debug("Quality computation request completed. DatasetURI: {}", datasetURI);
						
		} catch(Exception ex) {
			String errorTimeStamp = Long.toString((new Date()).getTime());
			logger.error("Error processing quality computation request [" + errorTimeStamp + "]", ex);
			
			// Build JSON response, indicating that an error occurred
			jsonResponse = buildJsonErrorResponse(datasetURI, errorTimeStamp, ex.getMessage());
		}
		
		isFree = true;
		//return jsonResponse;
		
		System.out.println("Finished Assessing: "+ datasetURI);
		return Response.ok(jsonResponse,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();

	}
	
	/**
	 * Returns the JSON message to be sent as response after successfully processing a quality computation request
	 * @param datasetURI URI of the dataset upon which the quality computation was performed
	 * @param qualityReport JENA model containing the quality report, computed. Null otherwise
	 * @return JSON representation of the response containing details about the quality computation
	 */
	private String buildJsonResponse(String datasetURI, Model qualityReport) {
		StringBuilder sbJsonResponse = new StringBuilder();
		sbJsonResponse.append("{ \"Dataset\": \"" + datasetURI + "\", ");
		sbJsonResponse.append("\"Agent\": \"" + Main.BASE_URI + "\", ");
		sbJsonResponse.append("\"Outcome\": \"SUCCESS\"");

		// If the quality report was generated, add its JSON representation to the response
//		if(qualityReport != null && !qualityReport.isEmpty()) {
//			// Serialize the quality report in JSON-LD format...
//			StringWriter strWriter = new StringWriter();
//			RDFDataMgr.write(strWriter, qualityReport, RDFFormat.JSONLD);
//			
//			// ... and append its JSON representation in the QualityReport field
//			sbJsonResponse.append(", \"QualityReport\": ");
//			sbJsonResponse.append(strWriter.toString());
//		}
		sbJsonResponse.append(" }");
		return sbJsonResponse.toString();
	}
	
	/**
	 * Returns the JSON message to be sent as response, when an exception or an error has occurred during the
	 * processing of the request
	 * @param datasetURI URI of the dataset upon which the request causing the error, was performed
	 * @param errorCode Code of the error identifying the exception occurrence
	 * @param errorMessage A user-friendly error message
	 * @return JSON representation of the response containing the error
	 */
	private String buildJsonErrorResponse(String datasetURI, String errorCode, String errorMessage) {
		StringBuilder sbJsonResponse = new StringBuilder();
		sbJsonResponse.append("{ \"Dataset\": \"" + datasetURI + "\", ");
		sbJsonResponse.append("\"Agent\": \"" + Main.BASE_URI + "\", ");
		sbJsonResponse.append("\"Outcome\": \"ERROR\", ");
		sbJsonResponse.append("\"ErrorMessage\": \"" + errorMessage + "\", ");
		sbJsonResponse.append("\"ErrorCode\": \"" + errorCode + "\" }");
		return sbJsonResponse.toString();
	}
}
