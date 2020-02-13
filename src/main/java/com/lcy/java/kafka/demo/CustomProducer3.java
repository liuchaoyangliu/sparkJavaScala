package com.lcy.java.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回 ack。
//由于 send 方法返回的是一个 Future 对象，根据 Futrue 对象的特点，我们也可以实现同
//步发送的效果，只需在调用 Future 对象的 get 方发即可

public class CustomProducer3 {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        
        Properties props = new Properties();
        
        props.put("bootstrap.servers", "hadoop102:9092");//kafka 集 群，broker-list
        
        props.put("acks", "all");
        
        props.put("retries", 1);//重试次数
        
        props.put("batch.size", 16384);//批次大小
        
        props.put("linger.ms", 1);//等待时间
        
        props.put("buffer.memory", 33554432);//RecordAccumulator 缓 冲区大小
        
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i))).get();
        }
        producer.close();
    }
}
