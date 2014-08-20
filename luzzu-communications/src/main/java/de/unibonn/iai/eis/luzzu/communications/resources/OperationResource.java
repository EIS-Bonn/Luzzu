package de.unibonn.iai.eis.luzzu.communications.resources;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unibonn.iai.eis.luzzu.datatypes.RankedElement;
import de.unibonn.iai.eis.luzzu.operations.ranking.AutomaticRanking;

@Path("/")
public class OperationResource {

	final static Logger logger = LoggerFactory.getLogger(OperationResource.class);
	
	@POST
	@Path("automatic_rank")
	@Produces(MediaType.APPLICATION_JSON)
	public String automaticRank(MultivaluedMap<String, String> formParams) {
		
		List<String> datasetURIs = null;
		List<String> chosenFilters = null;
		String jsonResponse = "";
		
		try {
			logger.info("Ranking Datasets: [{}] with filters: [{}]", formParams.get("Datasets"), formParams.get("Filters"));
			
			datasetURIs = formParams.get("Datasets");
			chosenFilters = formParams.get("Filters");
			
			if(datasetURIs == null) {
				throw new IllegalArgumentException("Datasets parameter was not provided.");
			}
			if  (datasetURIs.size() <= 0){
				throw new IllegalArgumentException("No dataset URI defined for ranking.");
			}
			if(chosenFilters == null) {
				throw new IllegalArgumentException("Filters parameter was not provided.");
			}
			
			Set<String> distinctFilters = new HashSet<String>();
			distinctFilters.addAll(chosenFilters);

			AutomaticRanking automaticRanking = new AutomaticRanking();
			List<RankedElement> rankedElements = automaticRanking.rank(datasetURIs, distinctFilters);
			jsonResponse = this.buildJsonResponse(rankedElements);
			
		} catch (Exception e){
			String errorTimeStamp = Long.toString((new Date()).getTime());
			logger.error("Error processing quality computation request [" + errorTimeStamp + "]", e);
			jsonResponse = buildJsonErrorResponse(errorTimeStamp, "The request caused an exception");
		}
		return jsonResponse;
	}
	
	private String buildJsonResponse(List<RankedElement> rankedElements) {
		StringBuilder sbRankedDS = new StringBuilder();
		int i = 1;
		for(RankedElement e : rankedElements){
			sbRankedDS.append("{");
			sbRankedDS.append("\"Position\": " + i);
			sbRankedDS.append("\"DatasetURI\": \"" + e.getDatasetURI() + "\"");
			sbRankedDS.append("\"Value\": " + e.getTotalRankValue());
			sbRankedDS.append("}");
			i++;
		}
		
		StringBuilder sbJsonResponse = new StringBuilder();
		sbJsonResponse.append("{ \"Ranked_datasets\": [ " + sbRankedDS + " ] ,");
		sbJsonResponse.append("\"Outcome\": SUCCESS");

		sbJsonResponse.append(" }");
		return sbJsonResponse.toString();
	}

	private String buildJsonErrorResponse(String errorCode, String errorMessage) {
		StringBuilder sbJsonResponse = new StringBuilder();
		sbJsonResponse.append("{ ");
		sbJsonResponse.append("\"Outcome\": ERROR, ");
		sbJsonResponse.append("\"ErrorMessage\": \"" + errorMessage + "\", ");
		sbJsonResponse.append("\"ErrorCode\": \"" + errorCode + "\" }");
		return sbJsonResponse.toString();
	}
	
}
