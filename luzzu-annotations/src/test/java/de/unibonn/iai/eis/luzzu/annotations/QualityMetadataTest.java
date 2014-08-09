package de.unibonn.iai.eis.luzzu.annotations;

import java.util.Arrays;
import java.util.Collection;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;

import de.unibonn.iai.eis.luzzu.assessment.QualityMetric;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.CUBE;
import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

/**
 * @author Jeremy Debattista
 *
 * The purpose of this test is to verify that the correct
 * quality metadata is being defined.
 * 
 */
@RunWith(Parameterized.class)
@PrepareForTest({DAQHelper.class, QualityMetric.class})
public class QualityMetadataTest extends Assert {
	
	@Rule public PowerMockRule rule = new PowerMockRule();
	
	private QualityMetric metric;
	private Model m = ModelFactory.createDefaultModel();
	private QualityMetadata _testClass;
	private Dataset _dataset;
	
	private Resource metricResource = m.createResource("example:mockedMetric");
	private Resource categoryResource = m.createResource("example:mockedCategory");
	private Resource dimensionResource = m.createResource("example:mockedDimension");
	private Resource dimensionProperty = m.createResource("example:hasMockedDimensionProperty");
	private Resource metricProperty = m.createResource("example:hasMockedMetricProperty");
	private Resource computedOn = m.createResource("example:testing");
	private Resource qualityGraphURI;
	
	private Resource categoryURI;
	private Resource dimensionURI;
	private Resource metricURI;
	
	private boolean parameter;
	
	@Parameters
	public static Collection<Object[]> data() {
		//TODO: Testing for Resources with cache
		Object[][] data = new Object[][] { {false},{ true } };
		return Arrays.asList(data);
	}

	public QualityMetadataTest(boolean QualityMetadataExists) throws Exception{
		this.parameter = QualityMetadataExists;
		_dataset = new DatasetImpl(m);
		if (QualityMetadataExists){
			String filename = this.getClass().getClassLoader().getResource("qualityMetadataTest.nquad").toExternalForm();
			_dataset = RDFDataMgr.loadDataset(filename,Lang.NQUADS);
		} 
	}
	

	@Before
	public void setUp() throws Exception{
		this.createMockedMetric();
		_testClass = new QualityMetadata(_dataset,computedOn);
		_testClass.addMetricData(metric);
		_dataset = _testClass.createQualityMetadata();
		
	}
	
	
	@Test
	public void oneQualityGraphDefined(){
		Model _default = _dataset.getDefaultModel();
		ResIterator iter = _default.listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
		assertEquals(1, iter.toList().size());
	}
	
	@Test
	public void correctQualityGraphDataStructure(){
		Model _default = _dataset.getDefaultModel();
		ResIterator iter = _default.listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
		this.qualityGraphURI = iter.next();
		assertTrue(_default.contains(this.qualityGraphURI, CUBE.structure, DAQ.dsd));
	}
	
	@Test
	public void oneDataStructureDefined(){
		Model _default = _dataset.getDefaultModel();
		this.setQualityGraphURI();
		StmtIterator iter2 = _default.listStatements(this.qualityGraphURI, CUBE.structure, DAQ.dsd);
		assertEquals(1, iter2.toList().size());
	}
	
	@Test
	public void containsQualityMetadataGraph(){
		this.setQualityGraphURI();
		assertTrue(_dataset.containsNamedModel(this.qualityGraphURI.getURI()));
	}
	
	@Test
	public void hasCategoryDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		assertTrue(_qualityGraph.contains(null, RDF.type, this.categoryResource));
		
		ResIterator iter = _qualityGraph.listSubjectsWithProperty(RDF.type, this.categoryResource);
		this.categoryURI = iter.next();
		
		NodeIterator iter2 = _qualityGraph.listObjectsOfProperty(m.createProperty(this.dimensionProperty.getURI()));
		this.dimensionURI = iter2.next().asResource();
		
		assertTrue(_qualityGraph.contains(this.categoryURI, m.createProperty(this.dimensionProperty.getURI()), this.dimensionURI));
	}
	
	@Test
	public void oneCategoryDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		ResIterator iter = _qualityGraph.listSubjectsWithProperty(RDF.type, this.categoryResource);
		assertEquals(1, iter.toList().size());
	}
	
	@Test
	public void hasDimensionDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		assertTrue(_qualityGraph.contains(this.dimensionURI, RDF.type, this.dimensionResource));
		
		NodeIterator iter2 = _qualityGraph.listObjectsOfProperty(m.createProperty(this.metricProperty.getURI()));
		this.metricURI = iter2.next().asResource();
		
		assertTrue(_qualityGraph.contains(this.dimensionURI, m.createProperty(this.metricProperty.getURI()) , this.metricURI));
	}
	
	@Test
	public void oneDimensionDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		ResIterator iter = _qualityGraph.listSubjectsWithProperty(RDF.type, this.dimensionResource);
		assertEquals(1, iter.toList().size());
	}
	
	@Test
	public void hasMetricDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		assertTrue(_qualityGraph.contains(this.metricURI, RDF.type, this.metricResource));
	}
	
	@Test
	public void oneMetricDefined(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		ResIterator iter = _qualityGraph.listSubjectsWithProperty(RDF.type, this.metricResource);
		assertEquals(1, iter.toList().size());
	}
	
	@Test
	public void hasNumberOfObservations(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		NodeIterator iter = _qualityGraph.listObjectsOfProperty(this.metricURI, DAQ.hasObservation);

		if (this.parameter){
			assertEquals(2, iter.toList().size());
		} else {
			assertEquals(1, iter.toList().size());
		}
	}
	
	@Test
	public void correctObservationStructure(){
		this.setQualityGraphURI();
		Model _qualityGraph = _dataset.getNamedModel(this.qualityGraphURI.getURI());
		NodeIterator iter = _qualityGraph.listObjectsOfProperty(this.metricURI, DAQ.hasObservation);
		while (iter.hasNext()){
			RDFNode node = iter.next();
			assertTrue(_qualityGraph.contains(node.asResource(), RDF.type, CUBE.Observation));
			assertTrue(_qualityGraph.contains(node.asResource(), DAQ.computedOn, this.computedOn));
			assertTrue(_qualityGraph.contains(node.asResource(), DC.date));
			assertTrue(_qualityGraph.contains(node.asResource(), DAQ.value));
			assertTrue(_qualityGraph.contains(node.asResource(), DAQ.metric, this.metricURI));
			assertTrue(_qualityGraph.contains(node.asResource(), CUBE.dataSet, this.qualityGraphURI));
		}
	}
	
	
	private void createMockedMetric() throws Exception{		
		metric = PowerMockito.mock(QualityMetric.class);
		PowerMockito.when(metric.getMetricURI()).thenReturn(metricResource);
		PowerMockito.when(metric.metricValue()).thenReturn(0.5);
	
		PowerMockito.mockStatic(DAQHelper.class);	
		PowerMockito.when(DAQHelper.getDimensionResource(metricResource)).thenReturn(dimensionResource);
		PowerMockito.when(DAQHelper.getPropertyResource(dimensionResource)).thenReturn(dimensionProperty);
		PowerMockito.when(DAQHelper.getPropertyResource(metricResource)).thenReturn(metricProperty);
		PowerMockito.when(DAQHelper.getCategoryResource(metricResource)).thenReturn(categoryResource);
	}
	
	private void setQualityGraphURI() {
		if (this.qualityGraphURI == null){
			Model _temp = _dataset.getDefaultModel();
			ResIterator iter = _temp.listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
			this.qualityGraphURI = iter.next();
		}
	}


}
