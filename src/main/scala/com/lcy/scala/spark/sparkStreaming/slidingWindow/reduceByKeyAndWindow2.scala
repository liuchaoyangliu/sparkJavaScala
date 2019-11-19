package com.lcy.scala.spark.sparkStreaming.slidingWindow

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 滑动窗口
  */
object reduceByKeyAndWindow2 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("reduceByKeyAndWindow").setMaster("local[2]")
    val jsc = new StreamingContext(conf, Seconds(5))

    jsc
      .socketTextStream("", 9999)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (v1, v2) => v1 + v2,
        (v1, v2) => v1 - v2,
        Seconds(15),
        Seconds(5)
      )
      .print()

    jsc.start()
    jsc.awaitTermination()
  }

}
