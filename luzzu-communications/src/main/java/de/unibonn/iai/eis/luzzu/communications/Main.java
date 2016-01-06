package de.unibonn.iai.eis.luzzu.communications;


import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.json.stream.JsonGenerator;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

public class Main {
	
	// Base URI the Grizzly HTTP server will listen on
	private static final Properties PROP = PropertyManager.getInstance().getProperties("webservice.properties");
	private static final String SCHEME = PROP.getProperty("SCHEME");
	private static final String DOMAIN = PROP.getProperty("DOMAIN");
	private static final String PORT_NUMBER = PROP.getProperty("PORT");
	private static final String APPLICATION = PROP.getProperty("APPLICATION");
	
	public static final String BASE_URI = SCHEME+"://"+DOMAIN+":"+PORT_NUMBER+"/"+ APPLICATION + "/";

	private static Map<String,Future<String>> computingResources = new ConcurrentHashMap<String, Future<String>>();
	private static Map<String,String> computeResourceDirectory = new ConcurrentHashMap<String, String>();
	private static Map<String,String> resourceToDatasetDirectory = new ConcurrentHashMap<String, String>();
	private static Set<String> finishedResources = new HashSet<String>();
	
	private static Set<String> successfulResources = new HashSet<String>();
	private static Set<String> failedResources = new HashSet<String>();

	private static ExecutorService executor = Executors.newFixedThreadPool(10);

	
    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        final ResourceConfig rc = new ResourceConfig().packages("de.unibonn.iai.eis.luzzu").property(JsonGenerator.PRETTY_PRINTING, true);
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
       return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
    	    	
    	// Start server and wait for user input to stop
        final HttpServer server = startServer();
        
        try {
            server.start();
            System.out.println(String.format("Jersey app started with WADL available at " + "%sapplication.wadl\n", BASE_URI));
            // Wait forever (i.e. until the JVM instance is terminated externally)
            Thread.currentThread().join();
        } catch (Exception ioe) {
            System.out.println("Error running Luzzu Communications service: " + ioe.toString());
        } finally {
        	if(server != null && server.isStarted()) {
        		server.shutdownNow();
        	}
        }
    }
    
    public static String getRequestStatus(String uuid) throws InterruptedException, ExecutionException{
    	if (computeResourceDirectory.get(uuid) != null){
    		if (!finishedResources.contains(uuid)){
        		Future<String> handler = computingResources.get(uuid);
        		if (handler.isDone()){
        			String result = handler.get();
        			computeResourceDirectory.remove(uuid);
        			computeResourceDirectory.put(uuid, result);
        			
        			JsonObject jobj = JSON.parse(result);
        			String outcome = jobj.get("Outcome").getAsString().value();
        			if (outcome.contains("SUCCESS")){
        				successfulResources.add(uuid);
        			} else {
        				failedResources.add(uuid);
        			}
        			
        			finishedResources.add(uuid);
        		} 
    		}
    		return computeResourceDirectory.get(uuid);
    	} else {
    		StringBuilder sb = new StringBuilder();
        	sb.append("{");
    		sb.append("\"Agent\": \"" + BASE_URI + "\", ");
        	sb.append("\"RequestID\": \"" + uuid + "\", ");
        	sb.append("\"Error\": \"Request ID not Found\"");
        	sb.append("}");
        	return sb.toString();
    	}
    }
    
    public static String addRequest(Callable<String> request, String datasetURI){
    	Future<String> handler = executor.submit(request);
    	String uuid = UUID.randomUUID().toString();
    	computingResources.put(uuid, handler);
    	resourceToDatasetDirectory.put(uuid, datasetURI);
    	
    	StringBuilder sb = new StringBuilder();
    	sb.append("{");
		sb.append("\"Agent\": \"" + BASE_URI + "\", ");
    	sb.append("\"RequestID\": \"" + uuid + "\", ");
    	sb.append("\"Dataset\": \"" + datasetURI + "\", ");
    	sb.append("\"Status\": \"In Progress\"");
    	sb.append("}");
    	
    	computeResourceDirectory.put(uuid, sb.toString());
    	return uuid;
    }
    
    public static String getAllPendingRequests(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("{");
		sb.append("\"PendingRequests\": [ ");

    	for (String s : resourceToDatasetDirectory.keySet()){
    		if (!finishedResources.contains(s)){
    	    	sb.append("{");
    			sb.append("\"RequestID\": \"" + s + "\", ");
    	    	sb.append("\"Dataset\": \"" + resourceToDatasetDirectory.get(s) + "\"");
    	    	sb.append("}");
    		}
    	}
    	sb.append("]");
    	sb.append("}");
    	return sb.toString();
    }
    
    public static String getAllSuccessfulRequests(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("{");
		sb.append("\"SuccessfulRequests\": [ ");

		for (String s : successfulResources){
			sb.append("{");
			sb.append("\"RequestID\": \"" + s + "\", ");
	    	sb.append("\"Dataset\": \"" + resourceToDatasetDirectory.get(s) + "\"");
	    	sb.append("}");
		}

		sb.append("]");
    	sb.append("}");
    	return sb.toString();
    }
    
    public static String getAllFailedRequests(){
    	StringBuilder sb = new StringBuilder();
    	sb.append("{");
		sb.append("\"FailedRequests\": [ ");

		for (String s : failedResources){
			sb.append("{");
			sb.append("\"RequestID\": \"" + s + "\", ");
	    	sb.append("\"Dataset\": \"" + resourceToDatasetDirectory.get(s) + "\"");
	    	sb.append("}");
		}

		sb.append("]");
    	sb.append("}");
    	return sb.toString();
    }
    
    

}
