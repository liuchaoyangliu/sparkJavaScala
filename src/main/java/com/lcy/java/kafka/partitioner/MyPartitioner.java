package com.lcy.java.kafka.partitioner;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class MyPartitioner implements Partitioner {
    public MyPartitioner() {
    }
    
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }
    
    public void close() {
    }
    
    public void configure(Map<String, ?> configs) {
    }
}

