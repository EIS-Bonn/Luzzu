package de.unibonn.iai.eis.luzzu.semantics.utilities;

import com.hp.hpl.jena.rdf.model.Resource;

public class SPARQLHelper {

	public static String SELECT_STATEMENT = "SELECT [variables] WHERE { [whereClauses] }";
	
	private SPARQLHelper(){}
	
	public static String toSPARQL(Resource n){
		if (n != null && (n.isResource() || n.isLiteral())){
			if (n.isURIResource()) 
				return "<" + n.getURI() + ">";
			else if (n.isLiteral()) {
				String sparql = "'''" + sparqlEncode(n.asLiteral().toString()) + "'''";

				//does it have a Datatype Literal?
				if (n.asLiteral().getDatatypeURI() != null){
					sparql = sparql + "'''^^<" + n.asLiteral().getDatatypeURI().toString() + ">";
				}
				
				if (n.asLiteral().getLanguage() != ""){
					sparql = sparql + "'''@" + n.asLiteral().getLanguage();
				}
				return sparql;
			}
			else return null; //TODO: throws exception
		} else
			return null; //TODO: throws exception
	}

	private static String sparqlEncode(String raw)  {
		String result = raw;
		result = result.replace("\\","\\\\");
		result = result.replace("'","\\'");
		result = result.replace("\"","\\\"");
		return result;
	}
}
