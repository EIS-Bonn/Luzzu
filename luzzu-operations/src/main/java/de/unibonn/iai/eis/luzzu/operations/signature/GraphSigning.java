package de.unibonn.iai.eis.luzzu.operations.signature;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.sparql.core.Quad;

public class GraphSigning {
	
	private Set<String> hashSet = Collections.synchronizedSortedSet(new TreeSet<String>());

	
	public void addHash(Quad quad){
		// TODO 1. we should ignore blank nodes
		// TODO 2. we should ignore certain triples 
		
		String subjectHash = "";
		if (!(quad.getSubject().isBlank())){
			subjectHash = quad.getSubject().toString();
		}
		
		String propertyHash = quad.getPredicate().toString();
		
		String objectHash = "";
		if (!(quad.getObject().isBlank())){
			objectHash = quad.getObject().toString();
		}
		
		String graphHash = "";
		if (quad.getGraph() != null){
			graphHash = quad.getGraph().toString();
		}
		
		this.hashSet.add(DigestUtils.md5Hex(subjectHash+propertyHash+objectHash+graphHash));
	}
	
	public String retrieveHash(){
		StringBuilder sb = new StringBuilder();
		
		for (String s : this.hashSet){
			sb.append(s);
		}
		this.hashSet = Collections.synchronizedSortedSet(new TreeSet<String>());
		
		return DigestUtils.md5Hex(sb.toString());
	}
}
