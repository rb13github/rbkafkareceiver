package com.kafka.kafkareceiver.service;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.stereotype.Service;

@Service
public class KafkaReceiver {
	
	 private final Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);

	    //@KafkaListener(topics = "zenith_product_demo_topic", groupId = "group_id")
	    @KafkaListener(topics = "zenith_product_demo_topic")
	    public void consume(String message) throws IOException {
	    	System.out.println("#### -> KafkaReceiver Consumed message -> "+message);
	        logger.info(String.format("#### -> KafkaReceiver Consumed message -> %s", message));
	    }
	    
//	    @KafkaListener(topics = "topicName")
//	    public void listenWithHeaders(
//	      @Payload String message, 
//	      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
//	          System.out.println(
//	            "Received Message: " + message"
//	            + "from partition: " + partition);
//	    }
}