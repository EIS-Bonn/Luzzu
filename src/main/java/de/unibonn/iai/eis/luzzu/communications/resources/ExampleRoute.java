package de.unibonn.iai.eis.luzzu.communications.resources;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("/")
public class ExampleRoute {

	@POST
	@Path("compute_quality_2")
	@Produces(MediaType.TEXT_PLAIN)
	public String getExample(String message) {
		return "rest ok: param value " + message;
	}
	
	@GET
	@Path("json/")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getJSONExample(){
		return new Object();
	}

}