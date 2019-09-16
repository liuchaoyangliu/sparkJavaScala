package com.lcy.java.demo.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class HDFSWordCount {

    public static void main(String[] args) throws InterruptedException {
//        SparkConf conf = new SparkConf().setAppName("SparkStreamingStudy").setMaster("local[1]");
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//        jsc.textFileStream("hdfs://hadoop1:9000/streaming")
//                .flatMap(e -> Arrays.asList(e.split(",")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((value1, value2) -> value1 + value2)
//                .print();
//         jsc.start();
//         jsc.awaitTermination();
//         jsc.stop();


        SparkConf conf = new SparkConf().setAppName("ReadHDFS").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.textFileStream("hdfs://hadoop:9000/streamingTest")
                .flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((value1, value2) -> value1 + value2)
                .print();

         jsc.start();
         jsc.awaitTermination();
         jsc.close();

    }

}
