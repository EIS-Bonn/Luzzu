package de.unibonn.iai.eis.luzzu.io;

import java.io.File;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.io.impl.MemoryProcessor;
import de.unibonn.iai.eis.luzzu.io.impl.StreamProcessor;

public class ProcessorController {

	private static ProcessorController instance = null;
	
	private ProcessorController(){};
	
	public static ProcessorController getInstance(){
		if (instance == null) instance = new ProcessorController();
		
		return instance;
	}
	
	public IOProcessor decide(String baseURI, String datasetURI,
			boolean genQualityReport, Model modelConfig){
		
		long freeMemory = Runtime.getRuntime().freeMemory();
		System.out.println();
		System.out.println("free memory: "+(double)freeMemory / (1024.0*1024.0) + " MB");
		File file =new File(datasetURI);
		
		System.out.println("file size: " + (double) file.length() / (1024.0*1024.0) + " MB");
		
		if (file.length() <= ((double) freeMemory * (1.0 / 12.0))) // if it fits in 1/12 of the free memory, then we use a memory processor
			return new MemoryProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
		else 
			return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
	}

}
