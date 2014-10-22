package de.unibonn.iai.eis.luzzu.evaluation.settings;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import benchmark.generator.Generator;

//http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/Dataset/
public class DataGenerator {

	private static ByteArrayOutputStream baos = new ByteArrayOutputStream();
	private static PrintStream ps = new PrintStream(baos);
	private static PrintStream old = System.out;
    

    
	public static Integer generateData(int scaleFactor, String source){
		System.setOut(ps);

		if (!new File(scaleFactor+".nt").isFile()){
			Generator.main(new String[]{"-pc",String.valueOf(scaleFactor),"-fn",source});
		}
		
		baos.toString();
		
	    System.out.flush();
	    System.setOut(old);
		return getNumberOfTriples();
	}
	
	private static Integer getNumberOfTriples(){
		Pattern pattern = Pattern.compile("([0-9])+ \\btriples generated\\b");
		String str = baos.toString();
		Matcher m = pattern.matcher(str);
		String s = "";
		while (m.find()) {
		    s = m.group();
		}
		s = s.replace(" triples generated","");
		return Integer.valueOf(s);
	}
}
