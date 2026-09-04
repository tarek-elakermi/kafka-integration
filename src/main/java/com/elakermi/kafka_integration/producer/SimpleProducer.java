package com.elakermi.kafka_integration.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {


    private final Producer<Integer, String> producer;

    public SimpleProducer() {
        Properties props = new Properties();

        // broker connection
        props.put("bootstrap.servers","localhost:9092");

        // 2. Serializers (convert Java objects to bytes
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 3 Acknowledgment  configuration
        props.put("acks","1");

        producer = new KafkaProducer<>(props);
    }

    public Producer<Integer, String> getProducer() {
        return producer;
    }

    public void sendMessage(String topic, Integer key, String message) {
        ProducerRecord<Integer, String> record =
                new ProducerRecord<>(topic,key,message);

        producer.send(record);
        System.out.println("Message sent: key=" + key + ", value=" + message);
    }
}
