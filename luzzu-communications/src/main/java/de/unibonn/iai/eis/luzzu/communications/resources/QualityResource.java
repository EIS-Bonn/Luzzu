package de.unibonn.iai.eis.luzzu.communications.resources;

import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

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

/**
 * REST resource, providing the functionalities to assess the quality of datasets, 
 * with respect to a set of metrics of interest
 * @author slondono
 */
@Path("/")
public class QualityResource {
	
	final static Logger logger = LoggerFactory.getLogger(QualityResource.class);
	
	@POST
	@Path("status")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status(MultivaluedMap<String, String> formParams){
		String jsonResponse = "";
		String reqID = "";
		try{
			List<String> lstRequestID = formParams.get("RequestID");
			if(lstRequestID == null || lstRequestID.size() <= 0) {
				throw new IllegalArgumentException("Request ID was not provided");
			}
			
			reqID = lstRequestID.get(0);
			jsonResponse = Main.getRequestStatus(reqID);
			
		} catch (Exception e){
			String errorTimeStamp = Long.toString((new Date()).getTime());
			StringBuilder sb = new StringBuilder();
        	sb.append("{");
    		sb.append("\"Agent\": \"" + Main.BASE_URI + "\", ");
        	sb.append("\"Request ID\": \"" + reqID + "\", ");
        	sb.append("\"TimeStamp\": \"" + errorTimeStamp + "\", ");
    		sb.append("\"ErrorMessage\": \"" + e.getMessage() + "\"");
    		sb.append("}");
			jsonResponse = sb.toString();
			e.printStackTrace();
		}
		
		return Response.ok(jsonResponse.toString(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
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
		
		String jsonResponse = "";
		final String datasetURI;
		String _datasetURI = "";
		final String jsonStrMetricsConfig;
		final String baseURI;
		final boolean genQualityReport;
		
		try {
			logger.info("Quality computation request received for dataset: {}", formParams.get("Dataset"));
			
			// Extract and validate parameters
			final List<String> lstDatasetURI = formParams.get("Dataset");
			final List<String> lstQualityReportReq = formParams.get("QualityReportRequired");
			final List<String> lstMetricsConfig = formParams.get("MetricsConfiguration");
			final List<String> lstBaseUri = formParams.get("BaseUri");
			final List<String> lstIsSparql = formParams.get("IsSparql");

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
			Model _modelConfig = ModelFactory.createDefaultModel();
			RDFDataMgr.read(_modelConfig, new StringReader(jsonStrMetricsConfig), null, Lang.JSONLD);
			final Model modelConfig = _modelConfig;
			
			// Obtain the base URI of the resources, which is important as it will be used to name the quality metadata models
			baseURI = lstBaseUri.get(0);
			
			final boolean isSPARQLEndpoint = Boolean.parseBoolean(lstIsSparql.get(0));
			
			datasetURI = lstDatasetURI.get(0);
			_datasetURI = lstDatasetURI.get(0);
			
			Callable<String> newRequest = new Callable<String>(){
				@Override
				public String call() throws Exception {
					IOProcessor strmProc = null;
					String jsonResponse;
					try{
						if (isSPARQLEndpoint){
							
							strmProc = new SPARQLEndPointProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
						} else {
							if ((datasetURI.startsWith("http://")) || (datasetURI.startsWith("ftp://"))){
								strmProc = new StreamProcessor(baseURI, datasetURI, genQualityReport, modelConfig);
							}else {
								strmProc = ProcessorController.getInstance().decide(baseURI, datasetURI, genQualityReport, modelConfig);
								logger.debug("Chosen Processor: {}", strmProc.getClass().toString());
								System.out.println("Chosen Processor: "+strmProc.getClass().toString());
							}
						}

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
						ex.printStackTrace();
					}
					return jsonResponse.toString();
				}
				
			};
		
			String requestID = Main.addRequest(newRequest, _datasetURI);
			System.out.println("Added request: "+ requestID);
			jsonResponse = Main.getRequestStatus(requestID);
			
		} catch (Exception e){
			String errorTimeStamp = Long.toString((new Date()).getTime());
			jsonResponse = buildJsonErrorResponse(_datasetURI, errorTimeStamp, e.getMessage());
		}
		
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
