package com.lcy.java.kafka.producer;


import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {
    public MyProducer() {
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        
        for(int i = 0; i < 10000; ++i) {
            producer.send(new ProducerRecord("test", "atguigu--" + i)).get();
        }
        
        producer.close();
    }
}

