package com.lcy.java.demo.sparkStreaming.Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkSteamingKafka2 {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SparkSteamingKafka").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(8));

        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", "cm02.spark.com:9092");
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "jis");
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);

        //要监听的Topic，可以同时监听多个
        Collection<String> topics = Arrays.asList("test");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
        JavaPairDStream<String, String> stringStringJavaPairDStream =
                directStream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        stringStringJavaPairDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();


        /**

        SparkConf conf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "cm02.spark.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "jis");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topices = Arrays.asList("test");
        KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.Subscribe(topices, kafkaParams)
        )
                .mapToPair(line -> new Tuple2<>(line.key(), line.value()))
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.close();
         */

    }

}
