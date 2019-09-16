package com.lcy.scala.demo.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 如何监控目录
  *  Spark Streaming将监视目录dataDirectory并处理在该目录中创建的任何文件。
  *
  * 1 可以监视一个简单的目录，例如"hdfs://namenode:8040/logs/‘*’"。直接在这种路径下的所有文件将在发现时进行处理。
  *
  * 2 甲POSIX glob模式可以被提供，例如  这里，
  *  DStream将包含与模式匹配的目录中的所有文件。那就是：它是目录的模式，而不是目录中的文件。
  *
  *
  * 3 所有文件必须采用相同的数据格式。
  *
  * 4 根据文件的修改时间而不是创建时间，文件被视为时间段的一部分。
  *
  * 5 处理完毕后，对当前窗口中文件的更改不会导致重新读取文件。那就是：忽略更新。
  *
  *  6 目录下的文件越多，扫描更改所需的时间就越长 - 即使没有修改过任何文件。
  *
  * 7 如果使用通配符来标识目录，例如"hdfs://namenode:8040/logs/2016-*"，重命名整
  *  个目录以匹配路径，则会将该目录添加到受监视目录列表中。只有修改时间在当前窗口内
  *  的目录中的文件才会包含在流中。
  *
  * 8 调用FileSystem.setTimes() 修复时间戳是一种在稍后的窗口中拾取文件的方法，
  *  即使其内容未更改
  *
  */


object HDFSWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingStudy")
    val sc = new StreamingContext(conf,Seconds(5))

    val inDStream: DStream[String] = sc.textFileStream("hdfs://hadoop1:9000/streaming")
    val resultDStream: DStream[(String, Int)] =
      inDStream
        .flatMap(_.split(","))
        .map((_,1))
        .reduceByKey(_+_)

    resultDStream.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()

  }

}











