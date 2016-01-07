package de.unibonn.iai.eis.luzzu.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.io.impl.LargeStreamProcessor;
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
			
		long length = file.length();
		
		if (datasetURI.endsWith("gz")){
			RandomAccessFile raf;
			try {
				raf = new RandomAccessFile(file, "r");
				raf.seek(raf.length() - 4);
				int b4 = raf.read();
				int b3 = raf.read();
				int b2 = raf.read();
				int b1 = raf.read();
				length = Math.abs((b1 << 24) | (b2 << 16) + (b3 << 8) + b4);
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("file size: " + (double) length / (1024.0*1024.0) + " MB");

		
		if (length <= ((double) freeMemory * (1.0 / 100.0))) // if it fits in 1/100 of the free memory, then we use a memory processor
			return new MemoryProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
		else if (length <= ((double) freeMemory * (1.0 / 2.0))) // if it fits in 1/2 of the free memory, then we use a normal stream processor
			return new StreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig);
		else
			return new LargeStreamProcessor(baseURI,datasetURI,genQualityReport,modelConfig); //e.g for dbpedia etc
	}

}
