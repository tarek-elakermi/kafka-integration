package com.elakermi.kafka_integration.configs;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class PartitionerLogic implements Partitioner {


    @Override
    public int partition(String _topic,
                         Object _key,
                         byte[] bytes,
                         Object o1,
                         byte[] bytes1,
                         Cluster cluster) {

        // Step 1: Get all partitions for this topic
        List<PartitionInfo> partitions = cluster.partitionsForTopic(_topic);
        // Step 2: Get the number of partitions (e.g., 3)
        int numPartitions = partitions.size();


        //Handle Null key
        if(_key == null ) {
            System.out.println(" NULL KEY - Using default partition (0)");
            return 0;
        }

        // Step 3: Convert the key to integer
        // the key should be the same type as the producer
        int intKey = (Integer) _key;

        // Step 4: Calculate partition and if key is negative make it positive
        int partition = Math.abs(intKey) % numPartitions;

        System.out.println(
                "DEBUG: Topic=" + _topic +
                        ", Key=" + intKey +
                        ", Partitions=" + numPartitions +
                        ", Selected Partition=" + partition
        );

        return partition;
    }

    @Override
    public void close() {
        System.out.println("Closing the PartitionerLogic ");
    }

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("SimplePartitioner is configured with: " + map);

    }
}
