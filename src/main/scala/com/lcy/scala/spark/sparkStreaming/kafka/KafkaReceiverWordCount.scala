package com.lcy.scala.spark.sparkStreaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val conf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(20))

    //2.接入kafka数据源(如何访问kafka集群？zookeeper)
    val zkQuorm = "192.168.64.111,192.168.64.112,192.168.64.113"
    //访问组
    val groupID = "g1"
    //访问主题
    val topic = Map[String,Int]("wc"->1)

    KafkaUtils
      .createStream(ssc,zkQuorm,groupID,topic)
      .map(_._2)
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
