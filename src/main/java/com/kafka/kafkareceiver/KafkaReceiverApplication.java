package com.kafka.kafkareceiver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

import com.kafka.kafkareceiver.model.Greeting;

@SpringBootApplication
public class KafkaReceiverApplication {

	public static void main(String[] args) throws Exception  {
		
	
	 ConfigurableApplicationContext context=SpringApplication.run(KafkaReceiverApplication.class, args);
		
		MessageListener listener = context.getBean(MessageListener.class);
		
		 listener.latch.await(10, TimeUnit.SECONDS);
		 
		 listener.greetingLatch.await(10, TimeUnit.SECONDS);

		 context.close();
	}
	
	
	 @Bean
	    public MessageListener messageListener() {
	        return new MessageListener();
	    }
	 
	 public static class MessageListener {

	        private CountDownLatch latch = new CountDownLatch(3);
	        private CountDownLatch greetingLatch = new CountDownLatch(1);

	        @KafkaListener(topics = "${message.topic.name}", groupId = "foo",containerFactory = "fooKafkaListenerContainerFactory")
	        public void listenGroupFoo(String message) {
	            System.out.println("Received Message in group 'foo': " + message);
	            latch.countDown();
	        }
	        
	        @KafkaListener(topics = "${message.topic.name}")
	        public void listen(String message) {
	            System.out.println("Received Message  " + message);
	            latch.countDown();
	        }
	        
	        @KafkaListener(topics = "${message.topic.object.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	        public void greetingListener(Greeting greeting) {
	            System.out.println("Received greeting message: " + greeting);
	            this.greetingLatch.countDown();
	        }

	 }
}
