package de.unibonn.iai.eis.luzzu.cache.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.cache.CacheManager;

public class TemporaryGraphMetadataCacheObjectTest extends Assert {
	

	private Model m = ModelFactory.createDefaultModel();
	private CacheManager mgr = CacheManager.getInstance();
	
	@Before
	public void setUp() throws Exception{
		
	}
	
	@Test
	public void createNewCache() {
		mgr.createNewCache("test", 10000);
		
		assertTrue(mgr.cacheExists("test"));
	}
	
	@Test
	public void createObjectInCache() {
		mgr.createNewCache("test", 10000);
		
		TemporaryGraphMetadataCacheObject mdCObj = new TemporaryGraphMetadataCacheObject(m.createResource("urn:testMetadataGraph"));
		mdCObj.addTriplesToMetadata(Quad.create(m.createResource("urn:testMetadataGraph").asNode(), 
				m.createResource("urn:subject").asNode(),
				m.createResource("urn:predicate").asNode(),
				m.createResource("urn:object").asNode()));
		
		mgr.addToCache("test", "obj1", mdCObj);
		
		TemporaryGraphMetadataCacheObject tmp = (TemporaryGraphMetadataCacheObject) mgr.getFromCache("test", "obj1");
		
		assertEquals(1,tmp.getMetadataModel().size());
	}
}
