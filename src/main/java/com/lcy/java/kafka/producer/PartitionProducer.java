package com.lcy.java.kafka.producer;


import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class PartitionProducer {
    public PartitionProducer() {
    }
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.atguigu.partitioner.MyPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        
        for(int i = 0; i < 10; ++i) {
            producer.send(new ProducerRecord("first", "atguigu--" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                    
                }
            });
        }
        
        producer.close();
    }
}

