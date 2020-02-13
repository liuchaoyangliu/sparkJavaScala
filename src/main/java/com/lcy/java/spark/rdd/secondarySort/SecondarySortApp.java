package com.lcy.java.spark.rdd.secondarySort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.math.Ordered;

import java.io.Serializable;
import java.util.Objects;

public class SecondarySortApp {
    
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setAppName("SecondarySortApp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        sc.textFile("file:\\D:\\sparkData\\demo3.txt")
                .mapToPair(line -> {
                    String[] lineSplits = line.split(" ");
                    return new Tuple2<>(new SecondSort(Integer.valueOf(lineSplits[0]),
                            Integer.valueOf(lineSplits[1])), line);
                })
                .sortByKey()
                .foreach(res -> System.out.println(res._2));
        
        sc.close();
        
    }
    
}

/**
 * 数据
 * 12 23
 * 12 34
 * 23 34
 * 23 45
 * 45 23
 * 56 23
 */
