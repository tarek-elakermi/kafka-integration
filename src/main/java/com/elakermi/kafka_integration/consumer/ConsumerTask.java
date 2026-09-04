package com.elakermi.kafka_integration.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This class runs in a separate thread.
 * Each thread reads from ONE SPECIFIC partition.
 */
public class ConsumerTask implements Runnable{

    private final String topic;
    private final String groupId;
    private final int partition;
    private final KafkaConsumer<Integer, String> consumer;

    public ConsumerTask(String topic, String groupId, int partition) {
        this.topic = topic;
        this.groupId = groupId;
        this.partition = partition;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);


        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));

            System.out.println("Thread started for partition: " + partition);

            while (true) {
                ConsumerRecords<Integer, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    System.out.println("[Thread " + Thread.currentThread().getName() +
                            "] Partition: " + record.partition() +
                            ", Offset: " + record.offset() +
                            ", Key: " + record.key() +
                            ", Value: " + record.value());
                });
            }

        } catch (Exception e) {
            System.out.println("Thread for partition " + partition + " stopped.");
        } finally {
            consumer.close();
        }
    }
}
