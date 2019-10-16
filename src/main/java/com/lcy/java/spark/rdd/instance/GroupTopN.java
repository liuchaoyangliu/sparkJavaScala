package com.lcy.java.spark.rdd.instance;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

//此时取出来的top3 不是排序后的前三个，而是随机的三个

public class GroupTopN {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("study").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.textFile("file:\\D:\\sparkData\\demo.txt")
                .mapToPair(line ->{
                    String[] strings = line.split(",");
                    return new Tuple2<>(strings[0], Integer.parseInt(strings[1]));
                })
                .groupByKey()
                .mapToPair(e -> new Tuple2<>(e._1, IteratorUtils.toList(e._2.iterator()).subList(0, 3)))
                .foreach(e -> System.out.println(e._1 + " " + e._2));
        jsc.stop();

    }

}

/**
 * 数据
 * class1 90
 * class2 56
 * class1 87
 * class1 76
 * class2 88
 * class1 95
 * class1 74
 * class2 87
 * class2 67
 * class2 77
 */
