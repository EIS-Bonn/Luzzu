package de.unibonn.iai.eis.luzzu.communications;


import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import javax.json.stream.JsonGenerator;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

public class Main {
	
	private static final Properties PROP = PropertyManager.getInstance().getProperties("webservice.properties");
	private static final String SCHEME = PROP.getProperty("SCHEME");
	private static final String DOMAIN = PROP.getProperty("DOMAIN");
	private static final String PORT_NUMBER = PROP.getProperty("PORT");
	private static final String APPLICATION = PROP.getProperty("APPLICATION");
	
	// Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = SCHEME+"://"+DOMAIN+":"+PORT_NUMBER+"/"+ APPLICATION + "/";

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        final ResourceConfig rc = new ResourceConfig().packages("de.unibonn.iai.eis.luzzu").property(JsonGenerator.PRETTY_PRINTING, true);	;
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
        System.out.println(String.format("Jersey app started with WADL available at " + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        
        server.shutdown();
    }

}
