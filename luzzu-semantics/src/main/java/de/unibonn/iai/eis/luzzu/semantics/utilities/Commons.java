package de.unibonn.iai.eis.luzzu.semantics.utilities;

import java.util.Calendar;
import java.util.UUID;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;
import com.hp.hpl.jena.sparql.core.Quad;

public class Commons {

	private Commons(){}
	
	public static Resource generateURI(){
		String uri = "urn:"+UUID.randomUUID().toString();
		Resource r = ModelFactory.createDefaultModel().createResource(uri);
		return r;
	}
	
	public static Literal generateCurrentTime(){
		return ModelFactory.createDefaultModel().createTypedLiteral(Calendar.getInstance());
	}
	
	public static Literal generateDoubleTypeLiteral(double d){
		return ModelFactory.createDefaultModel().createTypedLiteral(d);
	}
	
	public static RDFNode generateRDFBlankNode(){
		return ModelFactory.createDefaultModel().asRDFNode(NodeFactory.createAnon());
	}
	
	public static RDFNode asRDFNode(Node n){
		ModelCom mc = new ModelCom(Graph.emptyGraph);
		return mc.asRDFNode(n);
	}
	
	public static Quad statementToQuad(Statement statement, Resource graph){
		return new Quad(statement.getSubject().asNode(), statement.getPredicate().asNode(), statement.getObject().asNode(), graph.asNode());
	}
}
