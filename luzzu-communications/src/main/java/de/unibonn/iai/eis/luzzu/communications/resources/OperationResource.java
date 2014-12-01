package de.unibonn.iai.eis.luzzu.communications.resources;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unibonn.iai.eis.luzzu.operations.datatypes.RankedElement;
import de.unibonn.iai.eis.luzzu.operations.ranking.RankBy;
import de.unibonn.iai.eis.luzzu.operations.ranking.UserDrivenRanking;

@Path("/")
public class OperationResource {

	final static Logger logger = LoggerFactory.getLogger(OperationResource.class);
	
	//Filters="uri|value..." RankBy="cat" or "dim" or "met"
	@POST
	@Path("user_rank")
	@Produces(MediaType.APPLICATION_JSON)
	public String automaticRank(MultivaluedMap<String, String> formParams) {
		
		List<String> datasetURIs = null;
		List<String> chosenFilters = null;
		String rankBy = null;
		String jsonResponse = "";
		
		try {
			logger.info("Ranking Datasets: [{}] with filters: [{}]", formParams.get("Datasets"), formParams.get("Filters"), formParams.get("RankBy"));
			
			datasetURIs = formParams.get("Datasets");
			chosenFilters = formParams.get("Filters");
			rankBy = formParams.getFirst("RankBy");
			
			RankBy rank = null;
			if (rankBy.contains("cat")) rank = RankBy.CATEGORY;
			if (rankBy.contains("dim")) rank = RankBy.DIMENSION;
			if (rankBy.contains("met")) rank = RankBy.METRIC;
		
			if(datasetURIs == null) {
				throw new IllegalArgumentException("Datasets parameter was not provided.");
			}
			if  (datasetURIs.size() <= 0){
				throw new IllegalArgumentException("No dataset URI defined for ranking.");
			}
			if(chosenFilters == null) {
				throw new IllegalArgumentException("Filters parameter was not provided.");
			}
			if(rank == null) {
				throw new IllegalArgumentException("Rank parameter is invalid. It should be cat/dim/met.");
			}
			
			Map<String, Float> weightFilter = new HashMap<String,Float>();

			for(String filter: chosenFilters){
				weightFilter.put(filter.split("|")[0], Float.valueOf(filter.split("|")[1]));
			}
			
			UserDrivenRanking ranking = new UserDrivenRanking(datasetURIs, weightFilter, rank);
			List<RankedElement> rankedElements = ranking.getSortedList();
			
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
