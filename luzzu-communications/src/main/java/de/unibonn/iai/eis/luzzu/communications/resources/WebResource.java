package de.unibonn.iai.eis.luzzu.communications.resources;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.web.assessment.MetricConfiguration;
import de.unibonn.iai.eis.luzzu.web.export.CSVExporter;
import de.unibonn.iai.eis.luzzu.web.ranking.Facets;
import de.unibonn.iai.eis.luzzu.web.visualise.Data;
import de.unibonn.iai.eis.luzzu.web.visualise.Wizard;

@Path("/framework/web/")
public class WebResource {

	final static Logger logger = LoggerFactory.getLogger(WebResource.class);

	/**
	 * returns a list of installed metrics
	 **/
	@GET
	@Path("get/configured/metrics")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLoadedMetrics(){
		Model m = MetricConfiguration.getAllMetrics();
				
		StringWriter strWriter = new StringWriter();
		RDFDataMgr.write(strWriter, m, RDFFormat.JSONLD);
		
		
		return Response.ok(strWriter.toString(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();

	}
	
	
	@GET
	@Path("get/facet/options")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getFacetOptions(){
		return Response.ok(Facets.getFacetOptions(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
	
	@POST
	@Path("post/wizard/option")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWizardOptions(MultivaluedMap<String, String> formParams){	
		String type = formParams.get("type").get(0);
		logger.info("Requesting a wizard view for type : {}", type);
		
		if (type.equals("one")){
			return Response.ok(Wizard.getAllCommonDatasetsMetrics(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
					.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
		}
		else if (type.equals("two")){
			return Response.ok(Wizard.getAllDatasetsWithMultipleObservations(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
					.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
		}
		else if (type.equals("three")){
			return Response.ok(Wizard.getAllDatasets(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
					.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
		}
		else
			return null;
	}
	
	
	@POST
	@Path("post/visualisation/dsvsm")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetVSMetricVisualisation(MultivaluedMap<String, String> formParams){	
		String metric = formParams.get("metric").get(0);
		String datasetsArrayString = formParams.get("datasets").get(0);
		
		datasetsArrayString = datasetsArrayString.replace("[","").replace("]","");
		
		List<String> datasets = Arrays.asList(datasetsArrayString.split(","));
		
		logger.info("Requesting a Dataset vs Metric view for {} on {}", metric, StringUtils.join(datasets.toArray()));
		
		String json = Data.getLatestValueForMetrics(metric, datasets);
		return Response.ok(json,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
	@POST
	@Path("post/visualisation/dsqualityvis")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetQualityValuesForVisualisation(MultivaluedMap<String, String> formParams){	
		String dataset = formParams.get("dataset").get(0);
		
		logger.info("Requesting a Dataset Quality View for {}", dataset);
		
		String json = Data.getLatestObservationForDataset(dataset);
		
		return Response.ok(json,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();

	}
	
	@POST
	@Path("post/visualisation/dstime")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetOverTimeVisualisation(MultivaluedMap<String, String> formParams){	
		String dataset = formParams.get("dataset").get(0);
		String metricsArrayString = formParams.get("metrics").get(0);
		
		metricsArrayString = metricsArrayString.replace("[","").replace("]","");
		
		List<String> metrics = Arrays.asList(metricsArrayString.split(","));
		
		logger.info("Requesting a Dataset Over Time view for {} on {}", dataset, StringUtils.join(metrics.toArray()));
		
		String json = Data.getObservationsForDataset(dataset, metrics);
		System.out.println(json);
		return Response.ok(json,MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
	@GET
	@Path("get/export/csv")
	@Produces(MediaType.TEXT_PLAIN)
	public Response exportAllToCSV(){	
		
		return Response.ok(CSVExporter.exportAllDatasets().toString(),MediaType.TEXT_PLAIN).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
				.header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
}
