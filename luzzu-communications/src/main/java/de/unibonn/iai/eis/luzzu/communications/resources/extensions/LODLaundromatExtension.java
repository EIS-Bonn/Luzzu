package de.unibonn.iai.eis.luzzu.communications.resources.extensions;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unibonn.iai.eis.luzzu.web.extensions.LODLaundromat;


@Path("/framework/web/")
public class LODLaundromatExtension {

	final static Logger logger = LoggerFactory.getLogger(LODLaundromatExtension.class);
		
	@GET
	@Path("get/statistics/quality")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getQualityStats(){
		return Response.ok(LODLaundromat.getQualityStats(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();

	}
	
	@POST
	@Path("post/visualisation/lodlaundromat-ext-sample")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSampleForLodLaundromat(MultivaluedMap<String, String> formParams){	
		String metric = formParams.get("metric").get(0);
				
		logger.info("Requesting a Sample for Metric view {}", metric);
		
		String json = LODLaundromat.getSampleForLodLaundromat(metric);
		return Response.ok(json,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
}
