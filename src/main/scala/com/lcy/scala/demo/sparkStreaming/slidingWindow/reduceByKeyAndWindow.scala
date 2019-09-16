package com.lcy.scala.demo.sparkStreaming.slidingWindow

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf

object reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {

    /**

    val conf = new SparkConf().setAppName("WindowHotWordS").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val searchWordPairDStream = ssc.socketTextStream("spark1", 9999)
      .map(line => line.split(" ")(1))
      .map(word => (word, 1))

    // reduceByKeyAndWindow
    // 第二个参数，是窗口长度，这是是60秒
    // 第三个参数，是滑动间隔，这里是10秒
    // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续计算
    // 而是只是放在那里
    // 然后，等待我们的滑动间隔到了以后，10秒到了，会将之前60秒的RDD，因为一个batch间隔是5秒，所以之前60秒，
    // 就有12个RDD，给聚合起来，然后统一执行reduceByKey操作
    // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对 某个DStream中的RDD
    // 每隔10秒钟，出来 之前60秒的收集到的单词的统计次数
    val searchWordCountsDStream =
    searchWordPairDStream
      .reduceByKeyAndWindow(
        (v1: Int, v2: Int) => v1 + v2,
        Seconds(60),
        Seconds(10)
      )

    val finalDStream = searchWordCountsDStream.transform(searchWordCountsRDD => {

      val top3 = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
        .sortByKey(false)
        .map(tuple => (tuple._1, tuple._2))
        .take(3)

      for (tuple <- top3) {
        println("result : " + tuple)
      }

      searchWordCountsRDD
    })

    finalDStream.print()

    ssc.start()
    ssc.awaitTermination()

      */

    val conf = new SparkConf().setAppName("reduceByKeyAndWindows").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
      .socketTextStream("local", 9999)
      .map(line => line.split(" ")(1))
      .map(e => (e, 1))
      .reduceByKeyAndWindow(
        (v1: Int, v2: Int) => v1 + v2,
        Seconds(60),
        Seconds(10)
      )
      .transform(serachWordCountRDD => {
        serachWordCountRDD.map(tuple => (tuple._2, tuple._1))
          .sortByKey(false)
          .map(tuple => (tuple._1, tuple._2))
          .take(3)
          .foreach(print)
        serachWordCountRDD
      })
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
