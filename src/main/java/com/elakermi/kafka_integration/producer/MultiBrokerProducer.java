package com.elakermi.kafka_integration.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.Properties;


@Component
public class MultiBrokerProducer {

    private final Producer<Integer, String> producer;

    public MultiBrokerProducer() {
        Properties props = new Properties();

        // 1. broker connection
        props.put("bootstrap.servers","localhost:9092,localhost:9094,localhost:9096");

        // 2. Serializers (convert Java objects to bytes
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 3. Partitioning logic
        props.put("partitioner.class","com.elakermi.kafka_integration.configs.PartitionerLogic");

        // =====================================================
        // 4. ACKNOWLEDGMENTS - How many replicas must confirm?
        // =====================================================
        // "0"  = Producer doesn't wait for any acknowledgment
        //        FASTEST but might lose data
        //
        // "1"  = Leader broker must confirm
        //        BALANCED (you had this before)
        //
        // "all" = ALL in-sync replicas must confirm
        //         SAFEST but slowest
        //
        // For multi-broker, use "all" for fault tolerance
        // =====================================================
        props.put("acks","1");

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, Integer key, String message) {

        ProducerRecord<Integer, String> record =
                new ProducerRecord<>(topic,key,message);

        producer.send(record, (metadata,exception) -> {
            if (exception == null ) {
                System.out.println(
                        "Message sent: key=" + key +
                                ", value=" + message +
                                ", partition=" + metadata.partition()
                );
            } else {
                System.err.println("Error: " + exception.getMessage());
            }
        });

    }

    public void close() {
        producer.flush();
        producer.close();

        System.out.println("Producer closed.");
    }









}
