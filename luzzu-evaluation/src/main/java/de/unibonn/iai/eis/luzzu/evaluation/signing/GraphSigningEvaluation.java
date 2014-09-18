package de.unibonn.iai.eis.luzzu.evaluation.signing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.perf4j.LoggingStopWatch;
import org.perf4j.StopWatch;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

import de.unibonn.iai.eis.luzzu.datatypes.Object2Quad;
import de.unibonn.iai.eis.luzzu.operations.signature.GraphSigning;

/**
 * @author Jeremy Debattista
 *
 * Performance Evaluation for the Graph Signing algorithm
 * using all DBPedia datasets
 * 
 */
public class GraphSigningEvaluation {
	
	// Variables for dataset loader
	private static PipedRDFIterator<?> iterator;
	private static PipedRDFStream<?> rdfStream;
	private static ExecutorService executor = Executors.newSingleThreadExecutor();
	private static GraphSigning graphSign = new GraphSigning();
	private static int totalTriples = 0;
	
	@SuppressWarnings("unchecked")
	private static void startStreamProcess(final String datasetURI){
		Lang lang  = RDFLanguages.filenameToLang(datasetURI);

		if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
			iterator = new PipedRDFIterator<Quad>();
			rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
		} else {
			iterator = new PipedRDFIterator<Triple>();
			rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
		}
		
		Runnable parser = new Runnable(){
			public void run() {
				RDFDataMgr.parse(rdfStream, datasetURI);
			}
		};
		
		executor.submit(parser); 
		
		while (iterator.hasNext()){
			totalTriples++;
			Object2Quad stmt = new Object2Quad(iterator.next());
			graphSign.addHash(stmt.getStatement());
		}
		System.out.println("Triples consumed up till " + datasetURI + ": " + totalTriples);
	}
	
	
	  
	public static void main(String [] args) {
//		List<String> datasetURIList = sitemapReader("http://wiki.dbpedia.org/sitemap");
		
		StopWatch stopWatch = new LoggingStopWatch("graphsigning - DBPedia");
//		for(String uri : datasetURIList){
//			startStreamProcess(uri);
//		}
		stopWatch.stop();
		
		System.out.println("Approximate Time Taken (ms) : " + stopWatch.getElapsedTime());
		
		executor.shutdown();
	}
}
