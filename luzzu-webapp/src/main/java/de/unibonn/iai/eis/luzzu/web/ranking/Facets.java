package de.unibonn.iai.eis.luzzu.web.ranking;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;
import de.unibonn.iai.eis.luzzu.semantics.configuration.InternalModelConf;


/**
 * @author Jeremy Debattista
 * 
 * This class handles all methods related to the 
 * loading of facet options such as the Category
 * Dimensions and Metrics
 *
 */
public class Facets {

	private static String metadataBaseDir = "";
	private static Dataset d = DatasetFactory.createMem();
	final static Logger logger = LoggerFactory.getLogger(Facets.class);
	
	private static Map<String,Set<String>> cd = new HashMap<String,Set<String>>();
	private static Map<String,Set<String>> dm = new HashMap<String,Set<String>>();
	private static Map<String,String> labels = new HashMap<String,String>();
	
	private static String cacheValue = "";
	private static Long cacheKey = null;
	
	static{
		PropertyManager props = PropertyManager.getInstance();
		// If the directory to store quality metadata and problem reports was not specified, set it to user's home
		if(props.getProperties("directories.properties") == null || 
				props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR") == null) {
			metadataBaseDir = System.getProperty("user.dir") + "/qualityMetadata";
		} else {
			metadataBaseDir = props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR");
			metadataBaseDir = metadataBaseDir.replaceFirst("^~",System.getProperty("user.home"));
		}
	}
	
	/**
	 * @return a JSON with the possible facet options
	 */
	public static String getFacetOptions(){
		
		if (cacheKey != null){
			if (cacheKey.longValue() == cacheKey.longValue()){
				return cacheValue;
			}
		}
		
		File fld = new File(metadataBaseDir);
		File[] listOfFiles = fld.listFiles();
		
		logger.info("Loading Quality Metadata");
		for(File file : listOfFiles){
			logger.info("Trying to load metadata for {}", file.getName());
			loadFile(file);
		}
		
		d.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String selectQuery = "";
		
		URL url = Resources.getResource("facets.sparql");
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), getFlatModel());

		ResultSet set = exec.execSelect();
		while(set.hasNext()){
			QuerySolution qs = set.next();
			String catURI = qs.get("category").asResource().toString();
			String dimURI = qs.get("dimension").asResource().toString();
			String metURI = qs.get("metric").asResource().toString();
			
			String cat = qs.get("category_name").asLiteral().toString();
			String dim = qs.get("dimension_name").asLiteral().toString();
			String met = qs.get("metric_name").asLiteral().toString();
			
			labels.put(catURI, cat);
			labels.put(dimURI, dim);
			labels.put(metURI, met);
			
			if (cd.containsKey(catURI)){
				cd.get(catURI).add(dimURI);
			} else {
				Set<String> lst = new HashSet<String>();
				lst.add(dimURI);
				cd.put(catURI, lst);
			}
			
			if (dm.containsKey(dimURI)){
				dm.get(dimURI).add(metURI);
			} else {
				Set<String> lst = new HashSet<String>();
				lst.add(metURI);
				dm.put(dimURI, lst);
			}			
		}
		
		String json = "{";
		json += "\"category\" : [";
		for(String cat : cd.keySet()){
			json += "{";
			json += "\"label\" : \"" + labels.get(cat) + "\",";
			json += "\"uri\" : \"" + cat + "\",";
			
			json += "\"dimension\" : [";
			for(String dim : cd.get(cat)){
				json += "{";
				json += "\"label\" : \"" + labels.get(dim) + "\",";
				json += "\"uri\" : \"" + dim + "\",";
				
				json += "\"metric\" : [";
				for(String met : dm.get(dim)){
					json += "{";
					json += "\"label\" : \"" + labels.get(met) + "\",";
					json += "\"uri\" : \"" + met + "\"";
					json += "},";
				}
				json = json.substring(0, json.length()-1);
				json += "]";
				
				json += "},";
			}
			json = json.substring(0, json.length()-1);
			json += "]";
			
			
			json += "},";
		}
		json = json.substring(0, json.length()-1);
		json += "]";
		json += "}";
		
		cacheKey = cacheKey();
		cacheValue = json;
		
		return json;
	}
	
	private static Model getFlatModel() {
		Model m = ModelFactory.createDefaultModel();
		
		Iterator<String> iter = d.listNames();
		while (iter.hasNext()){
			m.add(d.getNamedModel(iter.next()));
		}
		m.add(d.getDefaultModel());
		
		return m;
	}
	
	private static void loadFile(File fileOrFolder){
		if (fileOrFolder.isHidden()) return ;
		if (fileOrFolder.getPath().endsWith(".trig")){
			Dataset _ds = RDFDataMgr.loadDataset(fileOrFolder.getPath());
			
			Iterator<String> iter = _ds.listNames();
			while (iter.hasNext()){
				String name = iter.next();
				d.addNamedModel(name, _ds.getNamedModel(name));
			}
			
			d.getDefaultModel().add(_ds.getDefaultModel());
		}
		if (fileOrFolder.isDirectory()){
			File[] listOfFiles = fileOrFolder.listFiles();
			for(File file : listOfFiles){
				loadFile(file);
			}
		}
	}

	
	private static long folderSize(File fileOrFolder){
		long length = 0l;
		if (fileOrFolder.isHidden()) length += 0l;
		if (fileOrFolder.isFile()) length += fileOrFolder.length();
		if (fileOrFolder.isDirectory()){
			File[] listOfFiles = fileOrFolder.listFiles();
			for(File file : listOfFiles){
				length += folderSize(file);
			}
		}
		return length;
	}
	
	private static Long cacheKey(){
		File fld = new File(metadataBaseDir);
		File[] listOfFiles = fld.listFiles();
		
		long l = 0;
		for(File file : listOfFiles){
			l += folderSize(file);
		}
		
		return new Long(l);
	}
}
