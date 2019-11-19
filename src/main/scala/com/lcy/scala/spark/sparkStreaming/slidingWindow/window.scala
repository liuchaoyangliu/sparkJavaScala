package com.lcy.scala.spark.sparkStreaming.slidingWindow

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 该操作由一个DStream对象调用，传入一个窗口长度参数，一个窗口移动速率参数，
  * 然后将当前时刻当前长度窗口中的元素取出形成一个新的DStream。
  */
object window {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord")
    val jsc = new StreamingContext(conf, Durations.seconds(5))

    jsc.socketTextStream("", 9999)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .window(
        Durations.seconds(15),
        Durations.seconds(5)
      )
      .print()

    jsc.start()
    jsc.awaitTermination()
    jsc.stop()

  }

}
