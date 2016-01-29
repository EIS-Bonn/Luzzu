package de.unibonn.iai.eis.luzzu.semantics.utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.semantics.datatypes.Observation;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

public class ObservationHelper {
	
	public static List<Observation> extractObservations(Model qualityMD, Resource metric){
		List<Observation> lst = new ArrayList<Observation>(); 
		
		ResIterator itRes = qualityMD.listResourcesWithProperty(RDF.type, metric);
		if (!(itRes.hasNext())) return lst;
		Resource resNode = itRes.next();
		NodeIterator iter = qualityMD.listObjectsOfProperty(resNode, DAQ.hasObservation);
		while(iter.hasNext()){
			Resource res = iter.next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, qualityMD.createProperty("http://purl.org/linked-data/sdmx/2009/dimension#timePeriod")).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//get value
			Double value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getDouble();
			
			Observation obs = new Observation(res, date, value, null);
			lst.add(obs);
		}
		
		return lst;
	}
	
	private static Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		try{
			return sdf.parse(date);
		} catch (ParseException e){
			sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
			return sdf.parse(date);
		}
	}
	
	public static Observation getLatestObservation(List<Observation> observations){
		Collections.sort(observations);
		Collections.reverse(observations);
		return observations.get(0);
	}
}
