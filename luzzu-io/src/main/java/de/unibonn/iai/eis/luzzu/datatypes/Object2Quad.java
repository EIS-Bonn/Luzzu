package de.unibonn.iai.eis.luzzu.datatypes;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;;

public class Object2Quad {

	private Quad quad;
	private Triple triple;
	
	public Object2Quad(Object iterator){
		if (iterator instanceof Quad){
			this.quad = (Quad) iterator;
		}
		
		if (iterator instanceof Triple){
			this.triple = (Triple) iterator;
		}
	}
	
	public Quad getStatement(){
		if (quad == null){
			quad = new Quad(null, triple);
		}
		
		return quad;
	}
}
