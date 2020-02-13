package com.lcy.java.kafka.producer;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InterceptorProducer {
    public InterceptorProducer() {
    }
    
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ArrayList<String> interceptors = new ArrayList();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put("interceptor.classes", interceptors);
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        
        for(int i = 0; i < 10000; ++i) {
            producer.send(new ProducerRecord("first", "atguigu", "atguigu--" + i));
        }
        
    }
}

