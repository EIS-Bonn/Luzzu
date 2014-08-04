package de.unibonn.iai.eis.luzzu.communications.resources;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.jena.atlas.json.io.parser.JSONParser;
import org.apache.jena.atlas.json.io.parserjavacc.javacc.JSON_Parser;
import org.apache.jena.riot.lang.JsonLDReader;
import org.glassfish.json.JsonParserImpl;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
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

@Path("/")
public class QualityResource {

	@POST
	@Path("compute_quality")
	@Produces(MediaType.TEXT_PLAIN)
	public String computeQuality(MultivaluedMap<String, String> formParams) {
		//TODO: Logging (https://github.com/EIS-Bonn/Luzzu/issues/2)
		List<String> lstDatasetURI = formParams.get("Dataset");
		List<String> lstQualityReportReq = formParams.get("QualityReportRequired");
		List<String> lstMetricsConfig = formParams.get("MetricsConfiguration");
								
		if(lstDatasetURI == null || lstDatasetURI.size() <= 0) {
			throw new IllegalArgumentException("Dataset URI was not provided");
		}
		if(lstQualityReportReq == null || lstQualityReportReq.size() <= 0) {
			throw new IllegalArgumentException("Generate Quality Report not specified");
		}
		if(lstMetricsConfig == null || lstMetricsConfig.size() <= 0) {
			throw new IllegalArgumentException("Metrics configuration not provided");
		}
		
		String datasetURI = lstDatasetURI.get(0);
		String jsonStrMetricsConfig = lstMetricsConfig.get(0);
		boolean genQualityReport = Boolean.parseBoolean(lstQualityReportReq.get(0));
		
		System.out.println("Dataset URI: " + datasetURI);
		System.out.println("Quality Report: " + genQualityReport);
		System.out.println("Configuration parameters: " + jsonStrMetricsConfig);

		try {
			Object jsonObj = JsonUtils.fromString(jsonStrMetricsConfig);
			
			RDFDataset rdf = (RDFDataset)JsonLdProcessor.toRDF(jsonObj, new JsonLdOptions());			
			List<Quad> lst = rdf.getQuads("@default");
			
			Model modelConfig = ModelFactory.createDefaultModel();
			Resource rSubj = Commons.generateURI();
			
			for(Quad qd : lst) {								
				Property prop = modelConfig.createProperty(qd.getPredicate().getValue());
				Literal litVal = modelConfig.createLiteral(qd.getObject().getValue());
				modelConfig.add(rSubj, prop, litVal);
			}
						
			StreamProcessor strmProc = new StreamProcessor(datasetURI, genQualityReport, modelConfig);

		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonLdError jse) {
			// TODO Auto-generated catch block
			jse.printStackTrace();
		}
		
		return "OK processing started";
	}

}
