package de.unibonn.iai.eis.luzzu.operations.signature;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.codec.digest.DigestUtils;

import com.hp.hpl.jena.sparql.core.Quad;


//This is still Experimental
public class GraphSigning {
	
//	private Set<String> hashSet = Collections.synchronizedSortedSet(new TreeSet<String>());

	private IntAVLTreeSet hashSet = new IntAVLTreeSet();
	private XXHashFactory factory = XXHashFactory.fastestJavaInstance();	
	
	public void addHash(Quad quad) throws IOException{
		// TODO 1. we should ignore blank nodes
		// TODO 2. we should ignore certain triples 
		
		String subjectHash = "";
		if (!(quad.getSubject().isBlank())){
			subjectHash = quad.getSubject().toString();
		}
		
		String propertyHash = quad.getPredicate().toString();
		
		String objectHash = "";
		if (!(quad.getObject().isBlank())){
			objectHash = quad.getObject().toString();
		}
		
		String graphHash = "";
		if (quad.getGraph() != null){
			graphHash = quad.getGraph().toString();
		}
		
		this.hashSet.add(this.fastHashing(subjectHash+propertyHash+objectHash+graphHash));
		
	//	this.hashSet.add(DigestUtils.md5Hex(subjectHash+propertyHash+objectHash+graphHash));
	}
	
	public String retrieveHash(){
		StringBuilder sb = new StringBuilder();
		
		for (Integer s : this.hashSet){
			sb.append(s);
		}
//		this.hashSet = Collections.synchronizedSortedSet(new IntAVLTreeSet());
		
		return DigestUtils.md5Hex(sb.toString());
	}

	private Integer fastHashing(String strData) throws IOException{
		byte[] data = strData.getBytes();
		
		ByteArrayInputStream in = new ByteArrayInputStream(data);

		int seed = 0x9747b28c; // used to initialize the hash value, use whatever
		                       // value you want, but always the same
		
		StreamingXXHash32 hash32 = factory.newStreamingHash32(seed);
		byte[] buf = new byte[8]; // for real-world usage, use a larger buffer, like 8192 bytes
		for (;;) {
		  int read = in.read(buf);
		  if (read == -1) {
		    break;
		  }
		  hash32.update(buf, 0, read);
		}
		Integer hash = hash32.getValue();
		
		return hash;
	}
}
