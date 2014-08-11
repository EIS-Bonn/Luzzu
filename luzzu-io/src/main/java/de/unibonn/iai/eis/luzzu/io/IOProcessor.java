package de.unibonn.iai.eis.luzzu.io;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;

public interface IOProcessor {

	/**
	 * Initialise the io processor with the necessary in-memory objects and metrics
	 */
	void setUpProcess();
	
	/**
	 * Process the dataset for quality assessment
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void startProcessing() throws ProcessorNotInitialised;
	
	/**
	 * Cleans up memory from unused objects after processing is finished
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void cleanUp() throws ProcessorNotInitialised;
	
	Model retreiveQualityReport();
}
