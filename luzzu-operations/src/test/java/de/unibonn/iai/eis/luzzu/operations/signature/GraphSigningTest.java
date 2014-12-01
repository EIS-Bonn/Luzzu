package de.unibonn.iai.eis.luzzu.operations.signature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;

import de.unibonn.iai.eis.luzzu.operations.signature.GraphSigning;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

public class GraphSigningTest extends Assert {

	@Test
	public void createAndValidateHash_NoModification() throws IOException{
		Dataset d = RDFDataMgr.loadDataset(this.getClass().getClassLoader().getResource("ranking/dataset1.nq").toExternalForm());
		// create
		String creationHash = this.createHash(d);
		
		// validate
		String validationHash = this.createHash(d);
		
		assertEquals(creationHash, validationHash);
	}
	
	@Test
	public void createAndValidateHash_ModifyTriplePosition() throws IOException{
		Dataset d = RDFDataMgr.loadDataset(this.getClass().getClassLoader().getResource("ranking/dataset1.nq").toExternalForm());
		// create
		String creationHash = this.createHash(d);
		
		// validate
		GraphSigning gs = new GraphSigning();
		
		// - create - start with default model
		Model defaultModel = d.getDefaultModel();
		StmtIterator iter = defaultModel.listStatements();
		while (iter.hasNext()){
			Statement stmt = iter.next();
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource()));
		}
		
		// - create - named graphs
		Model graph = d.getNamedModel("urn:a50f843c-1649-4f52-bda0-28b0ead913d2");
		iter = graph.listStatements();
		List<Statement> stmts = new ArrayList<Statement>();
		while (iter.hasNext()){
			stmts.add(iter.next());
		}
		Collections.shuffle(stmts, new Random(System.nanoTime()));
		
		for(Statement stmt : stmts){
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource("urn:a50f843c-1649-4f52-bda0-28b0ead913d2")));
		}
		
		String validationHash =  gs.retrieveHash();
		assertEquals(creationHash, validationHash);
	}
	
	@Test
	public void createAndValidateHash_ModifyTripleData() throws IOException{
		Dataset d = RDFDataMgr.loadDataset(this.getClass().getClassLoader().getResource("ranking/dataset1.nq").toExternalForm());
		// create
		String creationHash = this.createHash(d);
		
		// validate
		GraphSigning gs = new GraphSigning();
		
		// - create - start with default model
		Model defaultModel = d.getDefaultModel();
		StmtIterator iter = defaultModel.listStatements();
		while (iter.hasNext()){
			Statement stmt = iter.next();
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource()));
		}
		
		// - create - named graphs
		Model graph = d.getNamedModel("urn:a50f843c-1649-4f52-bda0-28b0ead913d2");
		iter = graph.listStatements();
		List<Statement> stmts = new ArrayList<Statement>();
		while (iter.hasNext()){
			stmts.add(iter.next());
		}
		
		for(Statement stmt : stmts){
			if ((stmt.getSubject().toString().contains("urn:obs3")) && (stmt.getPredicate().toString().equals(DAQ.value.toString()))){
				stmt = new StatementImpl(stmt.getSubject(), stmt.getPredicate(), Commons.generateDoubleTypeLiteral(1.0d));
			}
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource("urn:a50f843c-1649-4f52-bda0-28b0ead913d2")));
		}
		
		String validationHash =  gs.retrieveHash();
		assertNotEquals(creationHash, validationHash);
	}
	
	private String createHash(Dataset d) throws IOException{
		GraphSigning gs = new GraphSigning();
		
		// - create - start with default model
		Model defaultModel = d.getDefaultModel();
		StmtIterator iter = defaultModel.listStatements();
		while (iter.hasNext()){
			Statement stmt = iter.next();
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource()));
		}
		
		// - create - named graphs
		Model graph = d.getNamedModel("urn:a50f843c-1649-4f52-bda0-28b0ead913d2");
		iter = graph.listStatements();
		while (iter.hasNext()){
			Statement stmt = iter.next();
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource("urn:a50f843c-1649-4f52-bda0-28b0ead913d2")));
		}
		
		return gs.retrieveHash();
	}
	
	@Ignore
	@Test
	public void bigDataSet() throws IOException{
		Dataset d = RDFDataMgr.loadDataset(this.getClass().getClassLoader().getResource("bigdata/dbpedia1.nq").toExternalForm());
		// create
		GraphSigning gs = new GraphSigning();
		
		// - create - start with default model
		Model defaultModel = d.getDefaultModel();
		StmtIterator iter = defaultModel.listStatements();
		while (iter.hasNext()){
			Statement stmt = iter.next();
			gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource()));
		}
		
		// - create - named graphs
		Iterator<String> it = d.listNames();
		while(it.hasNext()){
			String nm = it.next();
			Model graph = d.getNamedModel(nm);
			iter = graph.listStatements();
			while (iter.hasNext()){
				Statement stmt = iter.next();
				gs.addHash(Commons.statementToQuad(stmt, ModelFactory.createDefaultModel().createResource(nm)));
			}
		}
		
		//System.out.println(gs.retrieveHash());
	}
}
