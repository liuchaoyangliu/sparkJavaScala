package com.lcy.scala.spark.rdd.broadcastAccumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

// 自定义Accumulator
object Accumulator2 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("accumulator")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val count = new MyAccumulator
        sc.register(count, "user_count")

        sc.parallelize(Array("hello ", " world", " java ", " scala", " qwe"))
                .foreach(x => count.add(x))
        println(count.value)

    }

}



