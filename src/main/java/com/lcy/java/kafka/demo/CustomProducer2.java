package com.lcy.java.kafka.demo;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//带回调函数的 API
//回调函数会在 producer 收到 ack 时调用，为异步调用，该方法有两个参数，分别是
//RecordMetadata 和 Exception，如果 Exception 为 null，说明消息发送成功，如果
//Exception 不为 null，说明消息发送失败。
//注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。

public class CustomProducer2 {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        
        
        Properties props = new Properties();
        
        props.put("bootstrap.servers", "hadoop102:9092");//kafka 集 群，broker-list
        
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓 冲区大小
        props.put("buffer.memory", 33554432);
        
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i)),
                    new Callback() {
                        //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                        @Override
                        public void
                        onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception
                                    == null) {
                                System.out.println("success->" + metadata.offset());
                            } else {
                                exception.printStackTrace();
                            }
                        }
                    });
        }
        producer.close();
    }
}
