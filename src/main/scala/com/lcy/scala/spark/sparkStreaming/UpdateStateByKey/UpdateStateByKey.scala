package com.lcy.scala.spark.sparkStreaming.UpdateStateByKey

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.sparkContext.setLogLevel("ERROR")
        ssc.checkpoint("file:\\D:\\sparkData\\checkpoint")

        val pairs = ssc.socketTextStream("192.168.75.132", 9999)
                .flatMap(_.split(" "))
                .map(word => (word, 1))

        val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
            //      getOrElse(0) 相等于if else 如果存在该值则返回该值 否则返回 默认的值 在此处为0
            //     函数常量定义，返回类型是Some(Int)，表示的含义是最新状态
            Some(state.getOrElse(0) + values.sum)
        })

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()

        //    val conf = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]")
        //    val ssc = new StreamingContext(conf, Seconds(5))
        //    ssc.sparkContext.setLogLevel("ERROR")
        //    ssc.checkpoint("./")
        //
        //    ssc
        //      .socketTextStream("local", 9999)
        //      .flatMap(_.split(" "))
        //      .map(e => (e, 1))
        //      .updateStateByKey((values: Seq[Int], state: Option[Int]) =>
        //        Some(state.getOrElse(0) + values.sum))
        //      .print()
        //
        //    ssc.start()
        //    ssc.awaitTermination()
        //    ssc.stop()


    }

}
