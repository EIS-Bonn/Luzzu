package de.unibonn.iai.eis.luzzu.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import de.unibonn.iai.eis.luzzu.evaluation.settings.DataGenerator;
import de.unibonn.iai.eis.luzzu.evaluation.settings.EvaluationCase;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.impl.SparkStreamProcessorObserver;
import de.unibonn.iai.eis.luzzu.io.impl.SparkStreamProcessorPubSub;
import de.unibonn.iai.eis.luzzu.io.impl.StreamProcessorObserver;

public class Main {
	
	private static long tStart;
	private static long tEnd;
	
	private static List<EvaluationCase> eCases = new ArrayList<EvaluationCase>();
	private static int scalefactor[] = new int[]{24,57,128,199,256,666,1369,2089,2785,28453,70812,141000,284826};
	private static Map<Integer, Integer> generatedTriples = new HashMap<Integer,Integer>();
	static{
		generatedTriples.put(24, 9861);
		generatedTriples.put(57, 25730);
		generatedTriples.put(128, 51091);
		generatedTriples.put(199, 76010);
		generatedTriples.put(256, 99032);
		generatedTriples.put(666, 251428);
		generatedTriples.put(1369, 503996);
		generatedTriples.put(2089, 754624);
		generatedTriples.put(2785, 999972);
		generatedTriples.put(28453, 10011487);
		generatedTriples.put(70812, 24835339);
		generatedTriples.put(141000, 49013816);
		generatedTriples.put(284826, 98957876);
	}
	
	private static boolean firstTimeGeneration = false;

	private static void setUp(int metrics) throws ClassNotFoundException, IOException{
		for (int sf : scalefactor){
			System.out.println("Generating Case for scale factor "+ sf + " and " + metrics + " metrics");
			Integer triples;
			if (firstTimeGeneration){
				System.out.println("Generating Triples");
				triples = DataGenerator.generateData(sf, "bsbm-"+sf);
				generatedTriples.put(sf, triples);
			} else {
				triples = generatedTriples.get(sf);
			}
			EvaluationCase _case = new EvaluationCase("Metrics Initialised : " + metrics + "; Dataset: " + triples + " triples; Scale Factor: "+sf, new File("bsbm-"+sf+".nt").getPath(), metrics);
			_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
			_case.setTotalTriples(triples);
			eCases.add(_case);
		}
		firstTimeGeneration = false;
	}
	
	
	public static void main (String [] args) throws ProcessorNotInitialised, IOException, ClassNotFoundException{
		//create csv file
		File csv = new File("benchmark.csv");
		
		if (!(csv.isFile())) {
			csv.createNewFile();
			String header = "triples,metrics,average time(s),min time(s),max time(s)";
			FileUtils.write(csv, header, true);
			FileUtils.write(csv, System.getProperty("line.separator"), true);
		}
		
		//firstTimeGeneration = true;
		for (int metric = 0; metric <= 9 ; metric++ ){
			eCases = new ArrayList<EvaluationCase>();
			setUp(metric);
			int iterations = 4;
			
			for (EvaluationCase eCase : eCases){
				System.out.println("Evaluating " + eCase.getCaseName());
				System.out.println("=================================");
							
				//Run benchmark for 10 iterations + 3 cold starts
				for(int i = -2; i <= iterations; i++){
					SparkStreamProcessorPubSub p = new SparkStreamProcessorPubSub(eCase.getDatasetURI(), false, eCase.getMetricConfiguration()); 		// initiate stream processor

					// setup processor
					p.setUpProcess();
					
					if (i >= 1){
						//process
						tStart = System.currentTimeMillis();
						p.startProcessing();
						tEnd = System.currentTimeMillis();
						long difference = tEnd - tStart;
						eCase.setDifference(difference);
						System.out.println("Iteration # : " + i + " - " + (difference / 1000.0));
					} else {
						p.startProcessing();
						System.out.println("Cold Run.."+ (i + 2));
					}
					
					//cleanup
					p.cleanUp();
				}
				
				FileUtils.write(csv, eCase.toString(), true);
				FileUtils.write(csv, System.getProperty("line.separator"), true);
				
				eCase.storeEvaluationCase();
			}
		}
		
		SparkStreamProcessorPubSub.close();
	}
}
