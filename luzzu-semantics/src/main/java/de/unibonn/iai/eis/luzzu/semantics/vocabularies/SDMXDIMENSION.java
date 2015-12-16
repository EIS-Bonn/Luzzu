/* CVS $Id: $ */
package de.unibonn.iai.eis.luzzu.semantics.vocabularies; 
import com.hp.hpl.jena.rdf.model.*;
 
/**
 * Vocabulary definitions from src/main/resources/vocabularies/cube/sdmx-dimension.ttl 
 * @author Auto-generated by schemagen on 09 Dec 2015 11:42 
 */
public class SDMXDIMENSION {
    /** <p>The RDF model that holds the vocabulary terms</p> */
    private static Model m_model = ModelFactory.createDefaultModel();
    
    /** <p>The namespace of the vocabulary as a string</p> */
    public static final String NS = "http://purl.org/linked-data/sdmx/2009/dimension#";
    
    /** <p>The namespace of the vocabulary as a string</p>
     *  @see #NS */
    public static String getURI() {return NS;}
    
    /** <p>The namespace of the vocabulary as a resource</p> */
    public static final Resource NAMESPACE = m_model.createResource( NS );
    
    /** <p>The length of time that a person has lived or a thing has existed.</p> */
    public static final Property age = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#age" );
    
    /** <p>Legal, conjugal status of each individual in relation to the marriage laws 
     *  or customs of the country.</p>
     */
    public static final Property civilStatus = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#civilStatus" );
    
    /** <p>Monetary denomination of the object being measured.</p> */
    public static final Property currency = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#currency" );
    
    /** <p>The highest level of an educational programme the person has successfully 
     *  completed.</p>
     */
    public static final Property educationLev = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#educationLev" );
    
    /** <p>The time interval at which observations occur over a given time period.</p> */
    public static final Property freq = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#freq" );
    
    /** <p>Job or position held by an individual who performs a set of tasks and duties.</p> */
    public static final Property occupation = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#occupation" );
    
    /** <p>The country or geographic area to which the measured statistical phenomenon 
     *  relates.</p>
     */
    public static final Property refArea = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#refArea" );
    
    /** <p>The period of time or point in time to which the measured observation is intended 
     *  to refer.</p>
     */
    public static final Property refPeriod = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#refPeriod" );
    
    /** <p>The state of being male or female.</p> */
    public static final Property sex = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#sex" );
    
    /** <p>The period of time or point in time to which the measured observation refers.</p> */
    public static final Property timePeriod = m_model.createProperty( "http://purl.org/linked-data/sdmx/2009/dimension#timePeriod" );
    
}
