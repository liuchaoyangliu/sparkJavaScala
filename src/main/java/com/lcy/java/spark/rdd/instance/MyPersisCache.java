package com.lcy.java.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MyPersisCache {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TestStorgeLevel");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile("file:\\D:\\sparkData\\data2.txt");
        text = text.cache();

        Long startTime1 = System.currentTimeMillis();
        Long count1 = text.count();
        System.out.println(count1);
        Long endTime1 = System.currentTimeMillis();
        System.out.println(endTime1 - startTime1);

        Long startTime2 = System.currentTimeMillis();
        Long count2 = text.count();
        System.out.println(count2);
        Long endTime2 = System.currentTimeMillis();
        System.out.println(endTime2 - startTime2);

        Long startTime3 = System.currentTimeMillis();
        Long count3 = text.count();
        System.out.println(count3);
        Long endTime3 = System.currentTimeMillis();
        System.out.println(endTime3 - startTime3);
    }

}
