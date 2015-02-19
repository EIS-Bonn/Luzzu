package de.unibonn.iai.eis.luzzu.assessment;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;

/**
 * @author jdebattist
 * 
 */
public interface QualityMetric {

	/**
	 * This method should compute the metric.
	 * 
	 * @param The Quad <s,p,o,c> passed by the stream processor to the quality metric
	 */
	void compute(Quad quad);

	/**
	 * @return the value computed by the Quality Metric
	 */
	double metricValue();

	/**
	 * @return returns the daQ URI of the Quality Metric
	 */
	Resource getMetricURI();
	
	/**
	 * @return returns a typed ProblemList which will be used to create a "quality report" of the metric.
	 */
	ProblemList<?> getQualityProblems();
	
	
	/**
	 * @return returns true if the assessed metric returns an estimate result due to its probabilistic assessment technique (e.g. bloom filters)
	 */
	boolean isEstimate();
	
	
	/**
	 * An agent is required to keep provenance track.  We encourage the definition of an
	 * agent which can be accessible online, or as part of the metrics vocabulary.
	 * 
	 * @return returns the Agent URI that assessed the metric's observation
	 */
	Resource getAgentURI();
	
}
