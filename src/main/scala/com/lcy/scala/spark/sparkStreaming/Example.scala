package com.lcy.scala.spark.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 定义上下文后，您必须执行以下操作。
  *
  * 1 通过创建输入DStreams来定义输入源。
  * 2 通过将转换和输出操作应用于DStream来定义流式计算。
  * 3 开始接收数据并使用它进行处理streamingContext.start()。
  * 4 等待处理停止（手动或由于任何错误）使用streamingContext.awaitTermination()。
  * 5 可以使用手动停止处理streamingContext.stop()。
  */

object Example {

  def main(args: Array[String]): Unit = {

    //创建一个本地StreamingContext，其中有两个工作线程，批处理间隔为1秒。
    // master需要2个内核来防止出现饥饿的情况。
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR`")
    ssc.socketTextStream("192.168.75.132", 9999)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
