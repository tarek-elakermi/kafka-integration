package com.elakermi.kafka_integration;

import com.elakermi.kafka_integration.consumer.MultiThreadHLConsumer;
import com.elakermi.kafka_integration.consumer.SimpleHLConsumer;
import com.elakermi.kafka_integration.producer.MultiBrokerProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KafkaIntegrationApplication implements CommandLineRunner {

    private final MultiBrokerProducer producer;
    private final AdminClient adminClient;
    private SimpleHLConsumer consumer;

    public KafkaIntegrationApplication(
            MultiBrokerProducer producer,
            AdminClient adminClient
    ) {
        this.producer = producer;
        this.adminClient = adminClient;
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaIntegrationApplication.class, args);



        // sending data from application to one broker
//        SimpleProducer producer = new SimpleProducer();
//
//        producer.sendMessage("topic-1", 1, "Hello from my app producer!");
//        producer.sendMessage("topic-1", 2, "Second message from app");
//        producer.sendMessage("topic-1", 3, "Message without key");
//
//        // Close the producer to flush all pending messages
//        producer.getProducer().close();
//        System.out.println("All messages sent. Producer closed.");

        // sending data from application to multiple brokers
//        MultiBrokerProducer multiBrokerProducer = new MultiBrokerProducer();
//
//        System.out.println("\n--- Sending messages with different keys ---");
//        multiBrokerProducer.sendMessage("topic-1", 1, "Message with key 1");
//        multiBrokerProducer.sendMessage("topic-1", 2, "Message with key 2");
//        multiBrokerProducer.sendMessage("topic-1", 3, "Message with key 3");
//        multiBrokerProducer.sendMessage("topic-1", 4, "Message with key 4");
//        multiBrokerProducer.sendMessage("topic-1", 5, "Message with key 5");
//
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        multiBrokerProducer.close();

    }


    @Override
    public void run(String... args) throws Exception {

        // first
        createTopic();

        // second


        // third
        //startProducer();

        // fourth
        testMultithreadConsumer();



    }

    // 1- creating the topic with some configs
    private void createTopic() throws InterruptedException, ExecutionException {
        System.out.println("Checking if topic exists...");

        // Check if topic already exists
        boolean topicExists = adminClient.listTopics()
                .names()
                .get()
                .contains("topic-1");

        if (topicExists) {
            System.out.println(" Topic 'topic-1' already exists");
            return;
        }

        // Create the topic
        System.out.println("Creating topic...");
        NewTopic topic = new NewTopic("topic-1", 3, (short) 3);

        adminClient.createTopics(Collections.singletonList(topic))
                .all()
                .get();

        System.out.println(" Topic 'topic-1' created successfully!");
        adminClient.close();
    }

    // 2- Start consumer in background
    private void startConsumer() {
        consumer = new SimpleHLConsumer();
        Thread consumerThread = new Thread(() -> {
            consumer.consumeFromTopic("topic-1");
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
        System.out.println("Consumer started in background");
    }

    // 3- starting a producer
    private void startProducer(String... args) throws Exception {

        producer.sendMessage("topic-1",1, "msg 1");
        producer.sendMessage("topic-1",2, "msg 2");
        producer.sendMessage("topic-1",3, "msg 3");
        producer.sendMessage("topic-1",4, "msg 4");
        producer.sendMessage("topic-1",5, "msg 5");
        producer.sendMessage("topic-1",6, "msg 6");


        producer.close();


        /******** Interactive Part with the console *************/
//        Scanner scanner = new Scanner(System.in);
//
//        while (true) {
//            System.out.println(">");
//            String input = scanner.nextLine().trim();
//
//            if(input.equalsIgnoreCase("exit")){
//                break;
//            }
//
//            if (input.isEmpty()) {
//                continue;
//            }
//
//
//            if (input.contains("-")) {
//                String[] parts = input.split("-", 2);
//                try {
//                    int key = Integer.parseInt(parts[0].trim());
//                    String message = parts[1].trim();
//                    producer.sendMessage("topic-1", key, message);
//                } catch (NumberFormatException e) {
//                    System.out.println(" Key must be a number!");
//                }
//            } else {
//                producer.sendMessage("topic-1", null, input);
//            }
//
//        }
//        producer.close();
//        if (consumer != null) {
//            consumer.close();
//        }
//        scanner.close();
//        System.out.println("Goodbye!");
//        System.exit(0);

        /***********************************/

    }

    // 4- test multi-thread consumer
    private void testMultithreadConsumer() throws InterruptedException {
        System.out.println("\n=== TESTING MULTI-THREAD CONSUMER ===");

        MultiThreadHLConsumer consumer1 = new MultiThreadHLConsumer("topic-1","multi-thread-group");

        // start 3 threads one for each partition
        consumer1.startConsuming(3);
        // let it run for 10 seconds to see the messages
        Thread.sleep(10000);

        consumer1.shutDown();
        System.out.println("Multi-thread consumer test complete!");




    }


}
