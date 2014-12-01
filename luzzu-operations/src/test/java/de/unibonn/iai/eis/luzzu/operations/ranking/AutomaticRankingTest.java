package de.unibonn.iai.eis.luzzu.operations.ranking;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import com.hp.hpl.jena.rdf.model.Model;

import de.unibonn.iai.eis.luzzu.operations.datatypes.RankedElement;
import de.unibonn.iai.eis.luzzu.semantics.utilities.DAQHelper;


@PrepareForTest({AutomaticRanking.class, DAQHelper.class})
public class AutomaticRankingTest extends Assert {
	
	@Rule public PowerMockRule rule = new PowerMockRule();
	
	private AutomaticRanking instance = new AutomaticRanking();
	private List<String> datasets = new ArrayList<String>(); 
	
	private String metricA = "urn:metricA";
	private String metricB = "urn:metricB";
	private String metricC = "urn:metricC";
	private String metricD = "urn:metricD";
	private String metricE = "urn:metricE";
	private String metricH = "urn:metricH";
	
	public AutomaticRankingTest() throws Exception{
		datasets.add(this.getClass().getClassLoader().getResource("ranking/dataset1.nq").toExternalForm());
		datasets.add(this.getClass().getClassLoader().getResource("ranking/dataset2.nq").toExternalForm());
		datasets.add(this.getClass().getClassLoader().getResource("ranking/dataset3.nq").toExternalForm());
		datasets.add(this.getClass().getClassLoader().getResource("ranking/dataset4.nq").toExternalForm());
	}
	
	@Before
	public void setUp() throws Exception{
		this.initMockedMethods();
	}
	
	@Test
	public void noFilter(){
		List<RankedElement> rankedElements = instance.rank(datasets, new HashSet<String>());
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.4875, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.425, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.4125, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.3875, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricA(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricA);
		
		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.607142857, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.45, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.407142857, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.328571429, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricE(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricE);
		
		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.542857143, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.407142857, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.321428571, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.221428571, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricAB(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricA);
		metrics.add(metricB);
		
		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.672222222, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.544444444, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.411111111, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.383333333, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricABC(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricA);
		metrics.add(metricB);
		metrics.add(metricC);
		
		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.575, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.565, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.49, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.475, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricAE(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricA);
		metrics.add(metricE);
		
		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.438888889, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.433333333, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.422222222, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.327777778, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	@Test
	public void filter_MetricBDEH(){
		Set<String> metrics = new HashSet<String>();
		metrics.add(metricB);
		metrics.add(metricD);
		metrics.add(metricE);
		metrics.add(metricH);

		List<RankedElement> rankedElements = instance.rank(datasets, metrics);
		
		assertEquals(true,rankedElements.get(0).getDatasetURI().endsWith("dataset1.nq"));
		assertEquals(0.515, rankedElements.get(0).getTotalRankValue(), 0.0001);

		assertEquals(true,rankedElements.get(1).getDatasetURI().endsWith("dataset3.nq"));
		assertEquals(0.48, rankedElements.get(1).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(2).getDatasetURI().endsWith("dataset4.nq"));
		assertEquals(0.42, rankedElements.get(2).getTotalRankValue(), 0.0001);
		
		assertEquals(true,rankedElements.get(3).getDatasetURI().endsWith("dataset2.nq"));
		assertEquals(0.29, rankedElements.get(3).getTotalRankValue(), 0.0001);
	}
	
	private void initMockedMethods() throws Exception{	
		PowerMockito.mockStatic(DAQHelper.class);
		PowerMockito.when(DAQHelper.getNumberOfMetricsInDataSet(Mockito.any(Model.class))).thenReturn(8);
	}
}
