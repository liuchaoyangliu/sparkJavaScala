package com.lcy.scala.demo.sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  *
  * 1. 对DStream实施map操作，会转换成另外一个DStream
  * 2. DStream是一组连续的RDD序列，这些RDD中的元素的类型是一样的。DStream是一个时间上连续接收
  * 数据但是接受到的数据按照指定的时间（batchInterval）间隔切片，每个batchInterval都会构造一个RDD，
  * 因此，Spark Streaming实质上是根据batchInterval切分出来的RDD串，想象成糖葫芦，每个山楂就是一个
  * batchInterval形成的RDD
  * 3. 对DStream实施windows或者reduceByKeyAndWindow操作，也是转换成另外一个DStream（window操作是
  * stateful DStream Transformation）
  * 4. DStream同RDD一样，也定义了map,filter,window等操作，同时，对于元素类型为(K,V)的pair DStream，
  * Spark Streaming提供了一个隐式转换的类，PairStreamFunctions
  * 5. DStream内部有如下三个特性：
  * -DStream也有依赖关系，一个DStream可能依赖于其它的DStream(依赖关系的产生，同RDD是一样的)
  * -DStream创建RDD的时间间隔，这个时间间隔是不是就是构造StreamingContext传入的第三个参数？是的！
  * -在时间间隔到达后，DStream创建RDD的方法
  *
  */

object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkStreamingStudy").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sc = new StreamingContext(sparkContext, Seconds(2))

    val inDStream: ReceiverInputDStream[String] =
      sc.socketTextStream("hostName", 9999)
    inDStream.print()

    val resultDStream: DStream[(String, Int)] =
      inDStream
        .flatMap(_.split(","))
        .map((_, 1))
        .reduceByKey(_ + _)

    resultDStream.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()

  }
}
