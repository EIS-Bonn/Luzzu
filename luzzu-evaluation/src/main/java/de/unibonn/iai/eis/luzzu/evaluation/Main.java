package de.unibonn.iai.eis.luzzu.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import de.unibonn.iai.eis.luzzu.evaluation.settings.DataGenerator;
import de.unibonn.iai.eis.luzzu.evaluation.settings.EvaluationCase;
import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.impl.StreamProcessor;

public class Main {
	
	private static long tStart;
	private static long tEnd;
	
	private static List<EvaluationCase> eCases = new ArrayList<EvaluationCase>();
	//private static int metrics[] = new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13}; 
	private static int scalefactor[] = new int[]{24,57,128,199,256,666,1369,2089,2785,28453,70812,141000,284826};
	protected static String fileNames[] = new String[]{
		"ec_0_9861_091545.dat",
	    "ec_0_25730_091548.dat",
		"ec_0_51091_091555.dat",
		"ec_0_76010_091604.dat",
		"ec_0_99032_091614.dat",
		"ec_0_251428_091636.dat",
		"ec_0_503996_091717.dat",
		"ec_0_754624_091817.dat",
		"ec_0_999972_091935.dat",
		"ec_0_10011487_093209.dat",
		"ec_0_24835339_100613.dat",
		"ec_0_49013816_114828.dat",
		"ec_0_98957876_144830.dat"
	};
	
	private static int eCaseNumber = 14;
	
	private static void setUp(int metrics){
		//Evaluation Case #1 - approximately 10K triples (BSBM scale factor 24)
		Integer triples = DataGenerator.generateData(24, "bsbm-24");
		EvaluationCase _case = new EvaluationCase("Case 1 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-24.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #2 - approximately 25K triples (BSBM scale factor 57)
		triples = DataGenerator.generateData(57, "bsbm-57");
		_case = new EvaluationCase("Case 2 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-57.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #3 - approximately 50K triples (BSBM scale factor 128)
		triples = DataGenerator.generateData(128, "bsbm-128");
		_case = new EvaluationCase("Case 3 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-128.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #4 - approximately 75K triples (BSBM scale factor 199)
		triples = DataGenerator.generateData(199, "bsbm-199");
		_case = new EvaluationCase("Case 4 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-199.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #5 - approximately 100K triples (BSBM scale factor 256)
		triples = DataGenerator.generateData(256, "bsbm-256");
		_case = new EvaluationCase("Case 5 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-256.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #6 - approximately 250K triples (BSBM scale factor 666)
		triples = DataGenerator.generateData(666, "bsbm-666");
		_case = new EvaluationCase("Case 6 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-666.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #7 - approximately 500K triples (BSBM scale factor 1369)
		triples = DataGenerator.generateData(1369, "bsbm-1369");
		_case = new EvaluationCase("Case 7 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-1369.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #8 - approximately 750K triples (BSBM scale factor 2089)
		triples = DataGenerator.generateData(2089, "bsbm-2089");
		_case = new EvaluationCase("Case 8 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-2089.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #9 - approximately 1M triples (BSBM scale factor 2785)
		triples = DataGenerator.generateData(2785, "bsbm-2785");
		_case = new EvaluationCase("Case 9 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-2785.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #10 - approximately 10M triples (BSBM scale factor 28453)
		triples = DataGenerator.generateData(28453, "bsbm-28453");
		_case = new EvaluationCase("Case 10 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-28453.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #11 - approximately 25M triples (BSBM scale factor 70812)
		triples = DataGenerator.generateData(70812, "bsbm-70812");
		_case = new EvaluationCase("Case 11 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-70812.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #12 - approximately 50M triples (BSBM scale factor 141000)
		triples = DataGenerator.generateData(141000, "bsbm-141000");
		_case = new EvaluationCase("Case 12 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-141000.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
		//Evaluation Case #13 - approximately 100M triples (BSBM scale factor 284826)
		triples = DataGenerator.generateData(284826, "bsbm-284826");
		_case = new EvaluationCase("Case 13 - Metrics Initialised : " + metrics + "; Dataset " + triples + " triples", new File("bsbm-284826.nt").getPath(), metrics);
		_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with no initialised metrics and a dataset with "+ triples + " triples");
		_case.setTotalTriples(triples);
		eCases.add(_case);
		
	}
	
	private static void setUp2(int metrics) throws ClassNotFoundException, IOException{
		for (String str : fileNames){
			EvaluationCase _case = new EvaluationCase("evaluation_cases/"+str, metrics);
			_case.setCaseName("Case "+ (eCaseNumber) + " -  Metrics Initialised : " + metrics + "; Dataset " + _case.getTotalTriples() + " triples");
			_case.setCaseDescription("In this Evaluation Case, we evaluated the stream processor with "+ metrics +" initialised metrics and a dataset with "+  _case.getTotalTriples()  + " triples");
			eCases.add(_case);
			eCaseNumber++;
		}
	}
	
	private static void generateData(){
		DataGenerator.generateData(24, "bsbm-24");
		DataGenerator.generateData(57, "bsbm-57");
		 DataGenerator.generateData(128, "bsbm-128");
		 DataGenerator.generateData(199, "bsbm-199");
		 DataGenerator.generateData(256, "bsbm-256");
		 DataGenerator.generateData(666, "bsbm-666");
		 DataGenerator.generateData(1369, "bsbm-1369");
		 DataGenerator.generateData(2089, "bsbm-2089");
		 DataGenerator.generateData(2785, "bsbm-2785");
		 DataGenerator.generateData(28453, "bsbm-28453");
		 DataGenerator.generateData(70812, "bsbm-70812");
		 DataGenerator.generateData(141000, "bsbm-141000");
		 DataGenerator.generateData(284826, "bsbm-284826");
	}
	
	
	public static void main (String [] args) throws ProcessorNotInitialised, IOException, ClassNotFoundException{
		generateData();
		
		//create csv file
		File csv = new File("benchmark.csv");
		
		if (!(csv.isFile())) csv.createNewFile();
		
		String header = "triples,metrics,average time(s),min time(s),max time(s)";
		FileUtils.write(csv, header, true);
		FileUtils.write(csv, System.getProperty("line.separator"), true);
		
		for (int metric = 0; metric <=13 ; metric++ ){
			eCases = new ArrayList<EvaluationCase>();
			setUp2(metric);
			int iterations = 3;
			
			for (EvaluationCase eCase : eCases){
				System.out.println("Evaluating " + eCase.getCaseName());
				System.out.println("=================================");
				
				StreamProcessor p = new StreamProcessor(eCase.getDatasetURI(), false, eCase.getMetricConfiguration()); 		// initiate stream processor
				
				//Run benchmark for 10 iterations + 3 cold starts
				for(int i = -1; i <= iterations; i++){
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
					}
				}
				
				FileUtils.write(csv, eCase.toString(), true);
				FileUtils.write(csv, System.getProperty("line.separator"), true);
				
				eCase.storeEvaluationCase();
				
				//cleanup
				p.cleanUp();
			}
		}
	}
}
