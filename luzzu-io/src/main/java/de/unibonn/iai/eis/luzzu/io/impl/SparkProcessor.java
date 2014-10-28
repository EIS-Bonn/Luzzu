package de.unibonn.iai.eis.luzzu.io.impl;

import java.util.List;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;


public class SparkProcessor {

	private static SparkConf conf = new SparkConf().setAppName("Luzzu").setMaster("local[4]"); // TODO: fix appname and master
	private static JavaSparkContext sc = new JavaSparkContext(conf);
	private static Model m = ModelFactory.createDefaultModel();
	
	public static void parse(final List<Triple> _queue, String datasetURI){
//		sink.start();
		JavaRDD<String> datasetRDD = sc.textFile(datasetURI);
		
		JavaRDD<String> queue = datasetRDD.map(new Function<String, String>(){
			private static final long serialVersionUID = -44291655703031316L;

			public String call(String quadOrTriple){
				return quadOrTriple;
			}
		});
		
		queue.reduce(new Function2<String, String, String>(){
			private static final long serialVersionUID = 6618564349759850297L;

			public String call(String a, String b) {
//				if (sink instanceof PipedQuadsStream) sink.quad(SSE.parseQuad(a));
//				if (sink instanceof PipedTriplesStream) sink.triple(toTripleStmt(a));
				_queue.add(toTripleStmt(a));
				return b;
			}
		});
		
		
		//sink.finish();
	}
	
	private static Triple toTripleStmt(String stmt){
		//TODO: this is a very quick hack and it is only for the experimentation phase
		Triple t = null;
		
		String[] triples = stmt.split(" "); //assuming that s p o are separated by a space
		
		//subject
		String subject = triples[0].replace("<", "").replace(">", "");
		Resource _s = m.createResource(subject);
		
		String predicate = triples[1].replace("<", "").replace(">", "");
		Property _p = m.createProperty(predicate);
		
		String object = triples[2];
		if (object.startsWith("<") && object.endsWith(">")){
			// this is a resource
			object = object.replace("<", "").replace(">", "");
			Resource _o = m.createResource(object);
			t = new Triple(_s.asNode(), _p.asNode(), _o.asNode());
		} else {
			// object is a literal
			object = object.replace("\"", "");
			Literal _o = m.createLiteral(object);
			t = new Triple(_s.asNode(), _p.asNode(), _o.asNode());
		}
		
		return t;
	}
	
}
