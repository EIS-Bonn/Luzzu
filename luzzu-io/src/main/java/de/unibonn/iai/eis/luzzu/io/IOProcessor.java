package de.unibonn.iai.eis.luzzu.io;

import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.helper.IOStats;

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

	/**
	 * A workflow initiating the whole assessment process.
	 * Such method usually executes the setUpProcess and startProcessing
	 * methods. 
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void processorWorkFlow() throws ProcessorNotInitialised;
	
	
	/**
	 * Returns statistics related to the IO processor
	 * such as the number of processed statements
	 * 
	 * @return
	 * @throws ProcessorNotInitialised
	 */
	List<IOStats> getIOStats() throws ProcessorNotInitialised;
}
