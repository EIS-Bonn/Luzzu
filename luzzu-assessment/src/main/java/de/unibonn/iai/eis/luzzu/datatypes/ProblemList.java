package de.unibonn.iai.eis.luzzu.datatypes;

import java.util.ArrayList;
import java.util.Collection;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.exceptions.ProblemListInitialisationException;

/**
 * @author Jeremy Debattista
 *
 * This is a generic class that is used to create 
 * a list of problems. A problem list should be typed
 * to a Resource @see com.hp.hpl.jena.rdf.model.Resource
 * or a Quad @see com.hp.hpl.jena.sparql.core.Quad
 * 
 * @param <T> should be either a Resource or a Quad
 */
public class ProblemList<T> {

	private Collection<T> problemList;
	
	// Default constructor. Support for empty problem lists is useful in many cases to signal 
	// that assessing a metric did not revealed any quality problems
	public ProblemList() {
		this.problemList = new ArrayList<T>();
	}
	
	public ProblemList(Collection<T> problemList) throws ProblemListInitialisationException{
		T oneObject = problemList.iterator().next();
		if (!(oneObject instanceof Resource) && !(oneObject instanceof Quad) && !(oneObject instanceof Model)){ // this is a quick hack since java does not allow the inferencing of the generic type of class during run-time
			throw new ProblemListInitialisationException("A ProblemList should be typed to a Resource, Model or a Quad");
		} 
		this.problemList = problemList;
	}
	
	public Collection<T> getProblemList() {
		return problemList;
	}
}
