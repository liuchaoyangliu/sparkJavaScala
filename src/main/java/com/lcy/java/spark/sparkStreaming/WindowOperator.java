package com.lcy.java.spark.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 基于滑动窗口的热点搜索词实时统计
 */

public class WindowOperator {

    public static void main(String[] args) throws InterruptedException {

        /**

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TransFormBlackList");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.socketTextStream("", 9999)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(searchWord -> new Tuple2<>(searchWord, 1))
                .reduceByKeyAndWindow(
                        (v1, v2) -> v1 + v2,
                        (v1, v2) -> v1 - v2,
                        Durations.seconds(15),
                        Durations.seconds(5))
                .transformToPair(
                        value -> value.
                                mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                                .sortByKey(false)
                                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                )
                .print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();

         */

        SparkConf conf = new SparkConf().setAppName("windowOperator").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.socketTextStream("127.0.0.1", 9999)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKeyAndWindow(
                        (v1, v2) -> v1 + v2,
                        (v1, v2) -> v1 - v2,
                        Durations.seconds(15),
                        Durations.seconds(5)
                )
                .transformToPair(
                        value -> value.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                        .sortByKey(false)
                        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                )
                .print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();

    }

}
