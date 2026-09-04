package com.elakermi.kafka_integration.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleHLConsumer {

    private final KafkaConsumer<Integer, String> consumer;

    public SimpleHLConsumer() {

        Properties props = new Properties();

        // 1- connect to brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");

        //2- Consumer group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "first-consumer-group");


        //3- Deserializers keys and values (convert bytes back to objects)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        //4- Where to start reading from
        // "earliest" = read from beginning (--from-beginning) in terminal
        // "latest" = read only new messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //5- Auto commit offsets (Kafka tracks what is had been read)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        consumer = new KafkaConsumer<>(props);
    }

    public void consumeFromTopic(String topic) {
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("\n📨 Listening for messages from: " + topic);
        System.out.println("Press Ctrl+C to stop...\n");

        try {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    System.out.println(" Partition: " + record.partition() +
                            ", Offset: " + record.offset() +
                            ", Key: " + record.key() +
                            ", Value: " + record.value());
                });
            }
        } catch (Exception e) {
            // Expected when thread is interrupted
            System.out.println("Consumer stopped.");
        }
    }

    public void close() {
        consumer.close();
        System.out.println("Consumer closed.");
    }

}
