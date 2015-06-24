package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

public class DatasetLoader {
	
	final Logger logger = LoggerFactory.getLogger(DatasetLoader.class);

	public static DatasetLoader instance = null;
	
	private String metadataBaseDir = "";
	private Dataset d = DatasetFactory.createMem();
	
	private Map<String,String> dsToQG = new HashMap<String,String>();
	
	private String checksum = "";

	protected DatasetLoader(){
		PropertyManager props = PropertyManager.getInstance();
		// If the directory to store quality metadata and problem reports was not specified, set it to user's home
		if(props.getProperties("directories.properties") == null || 
				props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR") == null) {
			metadataBaseDir = System.getProperty("user.dir") + "/qualityMetadata";
		} else {
			metadataBaseDir = props.getProperties("directories.properties").getProperty("QUALITY_METADATA_BASE_DIR");
			metadataBaseDir = metadataBaseDir.replaceFirst("^~",System.getProperty("user.home"));
		}
		
		loadDatasets();
	}
	
	public static DatasetLoader getInstance(){
		if (instance == null){
			instance = new DatasetLoader();
		}
		
		if (instance.requiresReload()){
			instance = new DatasetLoader();
		}
		
		return instance;
	}
	
	private boolean requiresReload(){
		if (this.checksum().equals(checksum)) return false;
		return false;
	}
	
	public Dataset getInternalDataset(){
		return d;
	}
	
	public Map<String,String> getAllGraphs(){
		return this.dsToQG;
	}
	
	private void loadDatasets(){
		File fld = new File(metadataBaseDir);
		File[] listOfFiles = fld.listFiles();
		Arrays.sort(listOfFiles);
		
		String newCheckSum = "";
		
		logger.info("Loading Quality Metadata");
		for(File file : listOfFiles){
			logger.info("Trying to load metadata for {}", file.getName());
			loadFile(file);
			newCheckSum += checksumValue(file);
		}
		
		this.checksum = Hashing.md5().hashString(newCheckSum, Charset.defaultCharset()).toString();
	}
	
	private void loadFile(File fileOrFolder){
		if (fileOrFolder.isHidden()) return ;
		if (fileOrFolder.getPath().endsWith(".trig")){
			Dataset _ds = RDFDataMgr.loadDataset(fileOrFolder.getPath());
			
			String baseURI = fileOrFolder.getParent();
			baseURI = baseURI.replace("metadataBaseDir", "");
			
			Iterator<String> iter = _ds.listNames();
			while (iter.hasNext()){
				String name = iter.next();
				d.addNamedModel(name, _ds.getNamedModel(name));
				dsToQG.put(baseURI, name);
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
	
	private String checksum(){
		File fld = new File(metadataBaseDir);
		File[] listOfFiles = fld.listFiles();
		Arrays.sort(listOfFiles);
		
		String newCheckSum = "";
		
		logger.info("Checking Checksum");
		for(File file : listOfFiles){
			logger.info("Checksum for: {}", file.getName());
			newCheckSum += checksumValue(file);
		}
		
		return Hashing.md5().hashString(newCheckSum, Charset.defaultCharset()).toString();
	}
	
	private String checksumValue(File fileOrFolder){
		String s = "";
		if (fileOrFolder.isHidden()) return "" ;
		else if (fileOrFolder.isDirectory()){
			File[] listOfFiles = fileOrFolder.listFiles();
			for(File file : listOfFiles){
				s += checksumValue(file);
			}
		} else {
			try {
				s = Files.hash(fileOrFolder, Hashing.md5()).toString();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return s;
	}
}
