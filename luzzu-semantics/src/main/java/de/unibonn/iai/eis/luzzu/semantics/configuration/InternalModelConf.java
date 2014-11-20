package de.unibonn.iai.eis.luzzu.semantics.configuration;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.unibonn.iai.eis.luzzu.semantics.vocabularies.DAQ;

public class InternalModelConf {

	// creates an empty model for the default dataset - a dataset is readonly.
	private static Dataset semanticModel = DatasetFactory.create(ModelFactory
			.createDefaultModel());

	final static Logger logger = LoggerFactory
			.getLogger(InternalModelConf.class);

	static {
		// Loading DAQ ontology into memory
		Model temp = ModelFactory.createDefaultModel();
		temp.read(InternalModelConf.class.getResourceAsStream("/vocabularies/daq/daq.trig"), null, "N3");

		semanticModel.addNamedModel(DAQ.NS, temp);

		File externalsFolder = new File("externals/vocabs/");
		if (externalsFolder.exists()){
			File[] listOfOntologies = externalsFolder.listFiles();
			for (File ontology : listOfOntologies) {
				temp = ModelFactory.createDefaultModel();
				logger.debug("Loading ontology : {} ", ontology.getName());
				temp.read(ontology.getPath(), "N3");
				try{
					semanticModel.addNamedModel(guessNamespace(temp), temp);
				} catch (Exception e) {
					logger.debug("Could not load model " + ontology.getPath());
				}
			}	
		}
	}

	private static String guessNamespace(Model temp) {
		List<Resource> res = temp.listSubjectsWithProperty(RDFS.subClassOf, DAQ.Category).toList();
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		for (Resource r : res) {
			String ns = r.getNameSpace();
			tempMap.put(ns, (tempMap.containsKey(ns)) ? (tempMap.get(ns) + 1) : 1);
		}
		return (String) sortByValue(tempMap).keySet().toArray()[0];
	}

	public static Model getDAQModel() {
		return semanticModel.getNamedModel(DAQ.NS);
	}


	public static Model getFlatModel() {
		Model m = ModelFactory.createDefaultModel();
		
		Iterator<String> iter = semanticModel.listNames();
		while (iter.hasNext()){
			m.add(semanticModel.getNamedModel(iter.next()));
		}
		return m;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
