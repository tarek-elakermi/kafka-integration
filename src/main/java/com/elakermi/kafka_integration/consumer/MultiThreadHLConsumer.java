package com.elakermi.kafka_integration.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadHLConsumer {

    private final String topic;
    private final String groupId;
    private ExecutorService executorService;

    public MultiThreadHLConsumer(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
    }


    // get all partitions for a topic
    private List<PartitionInfo> getPartitionsForTopic() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        try(KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props)) {
            return consumer.partitionsFor(topic);
        }
    }

    // start consuming with multiple threads
    public void startConsuming(int threadCount) {
        // get all partitions for the specified topic
        List<PartitionInfo> partitionInfos = getPartitionsForTopic();
        int totalPartitions = partitionInfos.size();

        System.out.println("Topic '" + topic + "' has " + totalPartitions + " partitions");

        // Determine how many threads to actually use
        // we should not use more threads than partitions => it's a rule
        int actualThreadsCount = Math.min(threadCount,totalPartitions);

        // Create a thread pool with that size given by actualThreadsCount
        executorService = Executors.newFixedThreadPool(actualThreadsCount);

        // Assign one partition to each thread
        for (int i = 0; i < actualThreadsCount; i++) {
            // get partition number
            int partition = partitionInfos.get(i).partition();

            // Create a consumer task for this partition
            ConsumerTask task = new ConsumerTask(topic, groupId, partition);


            // submit the task to the thread pool
            executorService.submit(task);

            System.out.println(" Thread " + (i + 1) + " assigned to partition " + partition);
        }

        System.out.println("All " + actualThreadsCount + " threads started!");
        System.out.println("Listening for messages...");
    }


    // Stop all threads
    public void shutDown() {
        if(executorService != null){
            executorService.shutdownNow();
            System.out.println("All threads stopped");
        }
    }





}
