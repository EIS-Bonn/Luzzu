package de.unibonn.iai.eis.luzzu.io.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * @author Jeremy Debattista
 * 
 * Resolve External Metric Dependencies from POM files 
 *
 */
public class DependencyLoader {
    
    private RepositorySystem system = getNewRepositorySystem();
    private String pomFileName = "";
    
    final static Logger logger = LoggerFactory.getLogger(ExternalMetricLoader.class);
    
    public DependencyLoader(String pomFileName){
    	this.pomFileName = pomFileName;
    }
    
    /**
     * Resolves all dependencies for the external metrics
     */
    public void resolve(){
    	RepositorySystemSession session = MavenRepositorySystemUtils.newSession();
    	
    	LocalRepository localRepo = new LocalRepository("/tmp/luzzu/local-repo");
    	((DefaultRepositorySystemSession) session).setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    	try {
			List<POMDependency> dep = getPOMDependencies(pomFileName);
			for(POMDependency d : dep) installDependency(d.groupId, d.artifactId, d.version, session);
		} catch (ParserConfigurationException | SAXException | IOException | DependencyResolutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
    
    private List<POMDependency> getPOMDependencies(String fileName) throws ParserConfigurationException, SAXException, IOException{
    	List<POMDependency> lst = new ArrayList<POMDependency>();
    	File fXmlFile = new File(fileName);
    	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    	Document doc = dBuilder.parse(fXmlFile);
    	doc.getDocumentElement().normalize();
    	 
    	NodeList nList = doc.getElementsByTagName("dependency");
    	for (int temp = 0; temp < nList.getLength(); temp++) {
    		Node nNode = nList.item(temp);
    		Element eElement = (Element) nNode;
    		lst.add(new POMDependency(eElement.getElementsByTagName("groupId").item(0).getTextContent()
    				,eElement.getElementsByTagName("artifactId").item(0).getTextContent(),
    				eElement.getElementsByTagName("version").item(0).getTextContent()));
    	}
    	
    	return lst;
    }
    
    private RepositorySystem getNewRepositorySystem(){
    	   DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
           locator.addService( RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class );
           locator.addService( TransporterFactory.class, FileTransporterFactory.class );
           locator.addService( TransporterFactory.class, HttpTransporterFactory.class );
           
           return locator.getService( RepositorySystem.class );
    }
    
    private void installDependency(String groupId, String artifactId, String version, RepositorySystemSession session) throws DependencyResolutionException{
    	logger.info("Installing and Loading Dependency : {} - {} - {} ", groupId , artifactId, version);

    	Dependency dependency = new Dependency( new DefaultArtifact(groupId, artifactId, "", "jar", version ), "compile" );
    	RemoteRepository central = new RemoteRepository.Builder( "central", "default", "http://central.maven.org/maven2/" ).build();
  
    	CollectRequest collectRequest = new CollectRequest();
    	collectRequest.setRoot( dependency );
    	collectRequest.addRepository( central );

    	DependencyRequest dependencyRequest = new DependencyRequest();
    	dependencyRequest.setCollectRequest( collectRequest );

    	system.resolveDependencies(session, dependencyRequest ).getRoot();
    }

    private class POMDependency{
    	protected String groupId;
    	protected String artifactId;
    	protected String version;
    	
    	POMDependency(String groupId, String artifactId, String version){
    		this.groupId = groupId;
    		this.artifactId = artifactId;
    		this.version = version;
    	}
    }
}