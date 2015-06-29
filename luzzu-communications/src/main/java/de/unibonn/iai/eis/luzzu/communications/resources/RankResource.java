package de.unibonn.iai.eis.luzzu.communications.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

import de.unibonn.iai.eis.luzzu.operations.ranking.RankBy;
import de.unibonn.iai.eis.luzzu.operations.ranking.RankedObject;
import de.unibonn.iai.eis.luzzu.operations.ranking.Ranking;
import de.unibonn.iai.eis.luzzu.operations.ranking.RankingConfiguration;

@Path("/")
public class RankResource {

	final static Logger logger = LoggerFactory.getLogger(RankResource.class);
	
	@POST
	@Path("rank")
	@Produces(MediaType.APPLICATION_JSON)
	public Response rank(String message) {
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode;
		List<RankingConfiguration> conf = new ArrayList<RankingConfiguration>();
		try {
			rootNode = mapper.readTree(message);
			Iterator<JsonNode> iter = rootNode.elements();
			while(iter.hasNext()){
				JsonNode n = iter.next();
				Resource res = ModelFactory.createDefaultModel().createResource(n.get("uri").asText());
				
				RankBy type = n.get("type").asText().equals("category") ? RankBy.CATEGORY :
					n.get("type").asText().equals("dimension") ? RankBy.DIMENSION : RankBy.METRIC ;

				RankingConfiguration rc = new RankingConfiguration(res, type, n.get("weight").asDouble());
				conf.add(rc);
			}
			
			Ranking ranking = new Ranking();
			List<RankedObject> rank = ranking.rank(conf);
			
			String json = "{ \"ranking\" : [";
			for(RankedObject ro : rank){
				ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
				try {
					json += ow.writeValueAsString(ro) + ",";
				} catch (JsonProcessingException e) {
					logger.error("Error transforming to json : {}", e.getMessage());
				}
			}
			json = json.substring(0, json.length()-1);
			json += "]}";
			
			return Response.ok(json,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
					.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
		} catch (IOException e) {
			
			return Response.ok(buildJsonErrorResponse(e.getLocalizedMessage(),e.getMessage()),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
					.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
		}
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
