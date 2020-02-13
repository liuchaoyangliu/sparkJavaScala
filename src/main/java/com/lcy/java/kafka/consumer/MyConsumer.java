package com.lcy.java.kafka.consumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyConsumer {
    
    public MyConsumer() {
    }
    
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "atguigu0408");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList("test"));
        
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100L);
            Iterator var4 = consumerRecords.iterator();
            
            while(var4.hasNext()) {
                ConsumerRecord<String, String> consumerRecord = (ConsumerRecord)var4.next();
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
    }
}

