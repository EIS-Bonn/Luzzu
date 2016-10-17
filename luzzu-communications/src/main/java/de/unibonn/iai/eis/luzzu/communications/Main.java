package de.unibonn.iai.eis.luzzu.communications;


import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
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

import de.unibonn.iai.eis.luzzu.exceptions.ProcessorNotInitialised;
import de.unibonn.iai.eis.luzzu.io.helper.IOStats;
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
	
	private static Map<String, Callable<String>> callableDirectory = new ConcurrentHashMap<String, Callable<String>>();
	
	private static Set<String> successfulResources = new HashSet<String>();
	private static Set<String> failedResources = new HashSet<String>();

	private static ExecutorService executor = Executors.newFixedThreadPool(12);

	
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
    	callableDirectory.put(uuid, request);
    	
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
    	    	sb.append("},");
    		}
    	}
		sb.deleteCharAt(sb.lastIndexOf(","));

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
	    	sb.append("},");
		}
		sb.deleteCharAt(sb.lastIndexOf(","));


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
	    	sb.append("},");
		}
		sb.deleteCharAt(sb.lastIndexOf(","));

		sb.append("]");
    	sb.append("}");
    	return sb.toString();
    }
    
    public static boolean cancelRequest(String requestId) throws ProcessorNotInitialised{
    	if (computingResources.containsKey(requestId)){
    		Future<String> handler = computingResources.get(requestId);
    		
    		ExtendedCallable<String> callable = (ExtendedCallable<String>) callableDirectory.get(requestId);
    		callable.getIOProcessor().cancelMetricAssessment();
    		handler.cancel(true);

			failedResources.add(requestId);

		   	StringBuilder sb = new StringBuilder();
	    	sb.append("{");
			sb.append("\"Agent\": \"" + BASE_URI + "\", ");
	    	sb.append("\"RequestID\": \"" + requestId + "\", ");
	    	sb.append("\"Dataset\": \"" + resourceToDatasetDirectory.get(requestId) + "\", ");
	    	sb.append("\"Status\": \"Cancelled\"");
	    	sb.append("}");
    		
	    	finishedResources.add(requestId);
			computeResourceDirectory.put(requestId, sb.toString());
    		return true;
    	}
    	else
    		return false;
    }
    
    public static String getRequestStats(String requestId){
		StringBuilder sb = new StringBuilder();

    	if (computingResources.containsKey(requestId)){
    		ExtendedCallable<String> callable = (ExtendedCallable<String>) callableDirectory.get(requestId);
    		try {
    			List<IOStats> stats = callable.getIOProcessor().getIOStats();

		    	sb.append("{");
				sb.append("\"Agent\": \"" + BASE_URI + "\", ");
		    	sb.append("\"RequestID\": \"" + requestId + "\", ");
		    	sb.append("\"Stats\": \"" + "[");
		    	for (IOStats ios : stats){
		    		sb.append("{");
					sb.append("\"ClassName\": \"" + ios.getClassName() + "\", ");
					sb.append("\"TriplesProcessed\": \"" + ios.getTriplesProcessed() + "\"");
					
					if (!ios.getQmdStatus().equals("")){
						sb.append(",\"Status\": \"" + ios.getQmdStatus() + "\"");
					}
					else if (!ios.getQrStatus().equals("")){
						sb.append(",\"Status\": \"" + ios.getQrStatus() + "\"");
					} else {
						sb.append(",\"Status\": \"" + "Assessing Triples" + "\"");
					}
		    		sb.append("},");
		    	}
		    	if (stats.size() > 0) sb.deleteCharAt(sb.length() - 1);
		    	sb.append("]");
		    	sb.append("}");
			} catch (ProcessorNotInitialised e) {
	        	sb.append("{");
	    		sb.append("\"Agent\": \"" + BASE_URI + "\", ");
	        	sb.append("\"RequestID\": \"" + requestId + "\", ");
	        	sb.append("\"Error\": \"Cannot get Statistics\"");
	        	sb.append("}");
	        }
    	} else{
        	sb.append("{");
    		sb.append("\"Agent\": \"" + BASE_URI + "\", ");
        	sb.append("\"RequestID\": \"" + requestId + "\", ");
        	sb.append("\"Error\": \"Cannot get Statistics\"");
        	sb.append("}");
    	}
    	return sb.toString();

    }
    

}
