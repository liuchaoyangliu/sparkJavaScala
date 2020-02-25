package com.lcy.java.kafka.interceptor;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    
    int success;
    int error;
    
    public CounterInterceptor() {
    }
    
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }
    
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            ++this.success;
        } else {
            ++this.error;
        }
        
    }
    
    public void close() {
        System.out.println("success：" + this.success);
        System.out.println("error：" + this.error);
    }
    
    public void configure(Map<String, ?> configs) {
    }
}

