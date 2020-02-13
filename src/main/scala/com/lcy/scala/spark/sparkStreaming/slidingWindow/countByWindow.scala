package com.lcy.scala.spark.sparkStreaming.slidingWindow

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 返回指定长度窗口中的元素个数。
 */
object countByWindow {

    def main(args: Array[String]): Unit = {

        //    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord")
        //    val jsc = new StreamingContext(conf, Durations.seconds(5))
        //
        //    jsc.socketTextStream("", 9999)
        //      .flatMap(line => line.split(" "))
        //      .map(word => (word, 1))
        //      .countByWindow(
        //        Durations.seconds(15),
        //        Durations.seconds(5)
        //      )
        //      .print()
        //
        //    jsc.start()
        //    jsc.awaitTermination()


        val conf = new SparkConf().setAppName("countByWindows").setMaster("local[2]")
        val jsc = new StreamingContext(conf, Seconds(5))

        jsc.socketTextStream("local", 9999)
                .flatMap(line => line.split(" "))
                .map(e => (e, 1))
                .countByWindow(
                    Seconds(15),
                    Seconds(5)
                )
                .print()

        jsc.start()
        jsc.awaitTermination()


    }
}
