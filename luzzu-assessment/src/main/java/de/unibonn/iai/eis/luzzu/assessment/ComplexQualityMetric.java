package de.unibonn.iai.eis.luzzu.assessment;

import de.unibonn.iai.eis.luzzu.exceptions.AfterException;
import de.unibonn.iai.eis.luzzu.exceptions.BeforeException;

/**
 * @author Jeremy Debattista
 * 
 * This interface extends the "simpler" Quality Metric.
 * This gives us the possibility of creating more complex quality metrics
 * which require further processing of the data in the dataset.
 * 
 */
public interface ComplexQualityMetric extends QualityMetric {
	
	/**
	 * Implement this method if the quality metric
	 * requires any pre-processing. 
	 * 
	 * If pre-processing is required, it should be done
	 * here rather than in the constructor.
	 */
	 void before(Object ... args) throws BeforeException;
	
	/**
	 * Implement this method if the quality metric
	 * requires any post-processing
	 */
	void after(Object ... args) throws AfterException;
}
