package com.lcy.java.spark.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformOperation {
    
    public static void main(String[] args) throws InterruptedException {
    
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("transform");
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
//
//        //模拟黑名单
//        List<Tuple2<String, Boolean>> blackList = new ArrayList<>();
//        blackList.add(new Tuple2<>("yasks", true));
//        blackList.add(new Tuple2<>("xuruyun", false));
//        //将黑名单转换成RDD
//        final JavaPairRDD<String, Boolean> blackNameRDD =
//                jsc.sparkContext().parallelizePairs(blackList);
//
//        //接受socket数据源
//        JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("node5", 9999);
//        JavaPairDStream<String, String> pairNameList =
//                nameList.mapToPair(s -> new Tuple2<>(s.split(" ")[1], s));
//
//        pairNameList.transform(nameRDD ->
//                nameRDD.leftOuterJoin(blackNameRDD)
//                        .filter(tuple -> {
//                            if (tuple._2._2.isPresent()) {
//                                return !tuple._2._2.get();
//                            }
//                            return true;
//                        })
//                        .map(tuple -> tuple._2._1)
//        )
//                .print();
//
//        jsc.start();
//        jsc.awaitTermination();
//        jsc.stop();
        
        
        SparkConf conf = new SparkConf().setAppName("transForm").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        
        ArrayList<Tuple2<String, Boolean>> blackList = new ArrayList<>();
        blackList.add(new Tuple2<>("yasks", true));
        blackList.add(new Tuple2<>("xuruyun", false));
        
        final JavaPairRDD<String, Boolean> blackNameRDD =
                jsc.sparkContext().parallelizePairs(blackList);
        
        jsc.socketTextStream("127.0.0.1", 9999)
                .mapToPair(line -> new Tuple2<>(line.split(" ")[1], line))
                .transform(nameRDD ->
                        nameRDD.leftOuterJoin(blackNameRDD)
                                .filter(tuple -> {
                                    if (tuple._2._2.isPresent()) {
                                        return !tuple._2._2.get();
                                    }
                                    return true;
                                })
                                .map(tuple -> tuple._2._1))
                .print();
        
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
        
    }
    
}
