package com.lcy.java.spark.Window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class Window {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("TransFormBlackList");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaStreamingContext jssc=new JavaStreamingContext(sc, Durations.seconds(5));

        JavaPairDStream<String,Integer> dSTream = jssc.socketTextStream("",9999)
                .map(value -> value.split(" ")[0])
                .mapToPair(str -> new Tuple2<>(str,1));

        //第二个参数 窗口长度
        //第三个参数 滑动间隔
        //就是说 每个10秒将最近60秒的数据作为一个窗口
        JavaPairDStream<String,Integer> searchWorldCountDStream= dSTream.reduceByKeyAndWindow(
                (v1, v2) -> v1+v2,
                Durations.seconds(60),
                Durations.seconds(10));
        //执行transform  操作  根据搜索词进行排序  然后获取排名前三的搜索词

        JavaPairDStream<String,Integer> finalRDD=  searchWorldCountDStream.transformToPair(
                value -> value.mapToPair(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1))
                .sortByKey(false)
                .mapToPair(integerStringTuple2 -> new Tuple2<>(integerStringTuple2._2,integerStringTuple2._1)));

        finalRDD.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();

    }

}
