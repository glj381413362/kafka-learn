package com.kafka.glj.learn;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区
 * properties.put("partitioner.class","kafka.examples.demo.MyPartitioner");
 */
public class MyPartitioner implements Partitioner {

    private final AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (null == keyBytes || keyBytes.length<1) {
            return atomicInteger.getAndIncrement() % numPartitions;
        }
        int hash = 0;
        for (byte b : keyBytes) {
            hash = 31 * hash + b;
        }
        return hash % numPartitions;
    }

    @Override
    public void close() {}
}