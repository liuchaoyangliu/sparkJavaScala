package com.lcy.java.demo.rdd.broadcastAccumulator;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class BroadcastDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("study").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String word = "class1";
        Broadcast<String> broadcast = jsc.broadcast(word);
        jsc.textFile("file:\\D:\\sparkData\\words.txt")
                .filter(x -> x.contains(broadcast.value()))
                .foreach(e -> System.out.println(e));

        jsc.stop();

    }

}
