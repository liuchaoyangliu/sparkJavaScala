package com.lcy.java.demo.rdd.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount2 {

    public static void main(String[] args) {

        // 第一步：创建SparkConf对象,设置相关配置信息
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

         // 最简写法
        sc.textFile("file:\\D:\\sparkData\\data2.txt")
                .flatMap( e -> Arrays.asList(e.split(" ")).iterator() )
                .mapToPair( e -> new Tuple2<>(e, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach((e) -> System.out.println(e._1 + "  " + e._2));

        sc.close();

    }

}