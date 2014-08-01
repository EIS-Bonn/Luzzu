package de.unibonn.iai.eis.luzzu.datatypes;

import java.util.List;

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

	private List<T> problemList;
	
	public ProblemList(List<T> problemList) throws ProblemListInitialisationException{
		if (!(problemList.get(0) instanceof Resource) && !(problemList.get(0) instanceof Quad)){ // this is a quick hack since java does not allow the inferencing of the generic type of class during run-time
			throw new ProblemListInitialisationException("A ProblemList should be typed to a Resource or a Quad");
		} 
		this.problemList = problemList;
	}

	public List<T> getProblemList() {
		return problemList;
	}
}
