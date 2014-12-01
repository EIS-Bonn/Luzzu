package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

public class TriplePublisher implements Serializable {
	private static final long serialVersionUID = 7937360002166659060L;
	private static final Properties sparkProperties = PropertyManager.getInstance().getProperties("spark.properties");

	final static Logger logger = LoggerFactory.getLogger(TriplePublisher.class);
	
	private final static String EXCHANGE_NAME = "triples_publish";
	
	private static Connection connection;
	private static Channel channel;
	
	private static void connect() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(sparkProperties.getProperty("RABBIT_MQ_SERVER"));
        factory.setUsername(sparkProperties.getProperty("RABBIT_MQ_USERNAME"));
        factory.setPassword(sparkProperties.getProperty("RABBIT_MQ_PASSWORD"));
        factory.setVirtualHost(sparkProperties.getProperty("RABBIT_MQ_VIRTUALHOST"));
        factory.setPort(Integer.parseInt(sparkProperties.getProperty("RABBIT_MQ_PORT")));
        
		try {
			logger.debug("Connecting channel to MQ service...");
			
			connection = factory.newConnection();
			channel = connection.createChannel();
			
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
			
			logger.debug("OK connected to MQ service, exchange {} created", EXCHANGE_NAME);
		} catch (IOException e) {
			logger.error("IO Error connecting to MQ service", e);
		}
	}
	
	public TriplePublisher() {
		if(connection == null || channel == null || !connection.isOpen() || !channel.isOpen()) {
			connect();
		}
	}
		
	public void publishTriples(JavaRDD<String> datasetRDD) throws IOException {
			logger.debug("Initiating publication of triples on the queue...");

			datasetRDD.foreach(new VoidFunction<String>() {
				private static final long serialVersionUID = 7603190977649586962L;

				@Override
				public void call(String stmt) throws Exception {
					// publish triple (statement) into the exchange 
					if(stmt != null) {
						if(channel == null) {
							logger.warn("Channel was found to be null attempting to publish, reconnecting...");
							connect();
						}
						channel.basicPublish(EXCHANGE_NAME, "", null, stmt.getBytes());
					}
				}
			});
			
			logger.debug("All triples published on the queue. Processing metrics...");
	}
	
	public void close() {
		try {
			if(channel.isOpen()) {
				channel.close();			
			}					
			if(connection.isOpen()) {
				connection.close();
			}
		} catch (IOException e) {
			logger.warn("Error closing channel or connection to MQ service", e);
		}
	}

}
