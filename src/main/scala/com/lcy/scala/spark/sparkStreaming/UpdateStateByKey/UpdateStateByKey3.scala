package com.lcy.scala.spark.sparkStreaming.UpdateStateByKey

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object UpdateStateByKey3 {

    val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
        iter.map { case (word, current_count, history_count) =>
            (word, current_count.sum + history_count.getOrElse(0))
        }
    }

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("test_update").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("Z://check")
        val ssc = new StreamingContext(sc, Seconds(5))

        ssc.socketTextStream("local", 9998)
                .flatMap(_.split(" "))
                .map((_, 1))
                .updateStateByKey(
                    updateFunc,
                    new HashPartitioner(sc.defaultParallelism),
                    true)
                .print()

        ssc.start()
        ssc.awaitTermination()
    }

}
