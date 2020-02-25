package com.lcy.java.kafka.demo;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

//自动提交 offset 的代码：

public class CustomConsumer {
    
    public static void main(String[] args) {
        
        Properties props = new Properties();
        //连接的集群
        props.put("bootstrap.servers", "192.168.153.128:9092");
        // 消费者组
        props.put("group.id", "test");
        //开启自动提交
        props.put("enable.auto.commit", "true");
        //自动提交延迟
        props.put("auto.commit.interval.ms", "1000");
        //key value反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            //获取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            //解析并打印 consumerRecords
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }
    }
    public void demo(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("first"));
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record: records){
                System.out.println(record.offset() +
                        record.key() +
                        record.value());
            }
        }
    }
}
