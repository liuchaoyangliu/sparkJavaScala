package com.lcy.java.spark.rdd.broadcastAccumulator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class Accumulator {
    
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("CustomerBroadcast")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        sc.setLogLevel("ERROR");
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("ONE", "TWO", "THREE","ONE"));

        UserDefinedAccumulator count = new UserDefinedAccumulator();
        //将累加器进行注册
        sc.sc().register(count, "user_count");
        //随机设置值
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(s -> {
            int num = new Random().nextInt(10);
            return new Tuple2<>(s, s + ":" + num);
        });
        //foreach中进行累加
        pairRDD.foreach(tuple2 -> count.add(tuple2._2));
        System.out.println("the value of accumulator is:"+count.value());

    }
    
}
