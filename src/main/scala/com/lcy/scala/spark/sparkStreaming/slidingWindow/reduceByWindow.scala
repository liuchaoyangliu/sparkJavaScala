package com.lcy.scala.spark.sparkStreaming.slidingWindow

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}


/**
  * 类似于reduce操作，只不过这里不再是对整个调用DStream进行reduce操作，
  * 而是在调用DStream上首先取窗口函数的元素形成新的DStream，然后在窗口元素形成的DStream上进行reduce
  */
object reduceByWindow {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("reduceByWindow").setMaster("local[2]")
    val jsc = new StreamingContext(conf, Durations.seconds(5))

    jsc.socketTextStream("", 9999)
      .flatMap(line => line.split(" "))
      .reduceByWindow(
        (v1, v2) => v1 + "-" + v2,
        Seconds(3) ,
        Seconds(1))
      .print()

    jsc.start()
    jsc.awaitTermination()

  }

}
