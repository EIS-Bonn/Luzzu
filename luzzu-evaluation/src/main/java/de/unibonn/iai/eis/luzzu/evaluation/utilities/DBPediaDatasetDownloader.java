package de.unibonn.iai.eis.luzzu.evaluation.utilities;

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
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class DBPediaDatasetDownloader {
	
	public static List<String> sitemapReader(String siteMapURI) throws ParserConfigurationException, MalformedURLException, SAXException, IOException{
		List<String> uris  = new ArrayList<String>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(new URL(siteMapURI).openStream());
		NodeList nodeList = doc.getElementsByTagName("sc:dataDumpLocation");
		for(int i = 0; i < nodeList.getLength(); i++){
			uris.add(nodeList.item(i).getTextContent());
		}
		return uris;
	}
	
	private static void fileDownloader(URL url, File targetDir) throws IOException{
		if (!targetDir.exists()) {
	          targetDir.mkdirs();
	      }
		System.out.println("Downloading : " + url.toString());
	      InputStream in = new BufferedInputStream(url.openStream());
	      // make sure we get the actual file
	      String baseName = FilenameUtils.getBaseName(url.toString());
	      String extension = FilenameUtils.getExtension(url.toString());

	        
	      File n3 = new File(baseName+"."+extension);
	      OutputStream out = new BufferedOutputStream(new FileOutputStream(n3));
	      copyInputStream(in, out);
	      out.close();
//	      return unpackArchive(n3, targetDir);
	}
	
	@SuppressWarnings("resource")
//	private static File unpackArchive(File theFile, File targetDir) throws IOException {
//	      if (!theFile.exists()) {
//	          throw new IOException(theFile.getAbsolutePath() + " does not exist");
//	      }
//	      if (!buildDirectory(targetDir)) {
//	          throw new IOException("Could not create directory: " + targetDir);
//	      }
//	      BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(new FileInputStream(theFile));
//	      String fileName = targetDir+"/"+theFile.getName().replace(".bz2", "");
//	      System.out.println("Unpacking : " + fileName);
//	      copyInputStream(bzIn, new BufferedOutputStream(new FileOutputStream(fileName)));
//	     
//	      bzIn.close();
//	      return theFile;
//	  }

	  private static void copyInputStream(InputStream in, OutputStream out) throws IOException {
	      final byte[] buffer = new byte[5000];
	      int n = 0;
	      while (-1 != (n = in.read(buffer))) {
	    	    out.write(buffer, 0, n);
	    	}
	      out.close();
	      in.close();
	  }
	  
//	  private static boolean buildDirectory(File file) {
//	      return file.exists() || file.mkdirs();
//	  }
	  
	  public static void main(String [] args) throws MalformedURLException, IOException, ParserConfigurationException, SAXException{
			List<String> datasetURIList = sitemapReader("http://wiki.dbpedia.org/sitemap");
			String resourceFolder = DBPediaDatasetDownloader.class.getClassLoader().getResource("dbpedia/placeholder.txt").getPath().replace("placeholder.txt", "");
			for(String uri : datasetURIList){
				fileDownloader(new URL(uri), new File(resourceFolder));
			}
	  }

}
