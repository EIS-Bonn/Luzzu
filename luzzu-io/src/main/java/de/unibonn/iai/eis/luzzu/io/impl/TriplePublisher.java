package de.unibonn.iai.eis.luzzu.io.impl;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TriplePublisher implements Serializable {
	private static final long serialVersionUID = 7937360002166659060L;

	final static Logger logger = LoggerFactory.getLogger(TriplePublisher.class);
	
	// rabbitmq
	private final static String EXCHANGE_NAME = "triples_publish";
	
	private static Connection connection;
	private static Channel channel;
	
	private static void connect() {
		ConnectionFactory factory = new ConnectionFactory();
	 	factory.setHost("146.148.49.148");
        factory.setUsername("luzzu");
        factory.setPassword("luzzu");
        factory.setVirtualHost("luzzu");
        factory.setPort(5672);
        
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			
			// [slondono] - create an exchange to publish triples
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
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
				public void call(String stmt) {
					// publish triple (statement) into the exchange 
					try {
						channel.basicPublish(EXCHANGE_NAME, "", null, stmt.getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
