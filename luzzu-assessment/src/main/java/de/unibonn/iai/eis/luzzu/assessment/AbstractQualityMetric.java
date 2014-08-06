package de.unibonn.iai.eis.luzzu.assessment;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;
import de.unibonn.iai.eis.luzzu.datatypes.ProblemList;

@Deprecated
public abstract class AbstractQualityMetric implements QualityMetric{
	
	public abstract void compute(Quad quad);
	public abstract double metricValue();
	public abstract Resource getMetricURI();
	public abstract ProblemList<?> getQualityProblems();
}
