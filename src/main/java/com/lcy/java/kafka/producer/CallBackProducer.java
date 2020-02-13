package com.lcy.java.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CallBackProducer {
    public CallBackProducer() {
    }
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        
        for(int i = 0; i < 10; ++i) {
            producer.send(new ProducerRecord("aaa", "atguigu", "atguigu--" + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
                
            });
        }
        
        producer.close();
    }
}

