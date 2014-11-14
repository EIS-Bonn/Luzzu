package de.unibonn.iai.eis.luzzu.cache.impl;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.cache.CacheObject;
import de.unibonn.iai.eis.luzzu.semantics.utilities.Commons;

/**
 * @author Jeremy Debattista
 * 
 * Datastructure for caching Quality Metadata Graphs
 *  
 */
public class TemporaryGraphMetadataCacheObject implements CacheObject {

	/**
	 * Serial Version
	 */
	private static final long serialVersionUID = -286271367636630291L;
	
	transient private Resource graphURI = null;
	transient private Model metadataModel = null;
	
	public TemporaryGraphMetadataCacheObject(Resource graphURI){
		this.graphURI = graphURI;
		this.metadataModel = ModelFactory.createDefaultModel();
	}
	
	public void addTriplesToMetadata(Quad quad){
		Property p = metadataModel.createProperty(quad.getPredicate().getURI());
		metadataModel.add(Commons.asRDFNode(quad.getSubject()).asResource(), p, Commons.asRDFNode(quad.getObject()));
	}
	
	public void addModelToMetadata(Model model){
		metadataModel.add(model);
	}
	
	public Resource getGraphURI(){
		return this.graphURI;
	}
	
	public Model getMetadataModel(){
		return this.metadataModel;
	}
}
