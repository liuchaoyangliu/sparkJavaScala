package com.lcy.scala.spark.sparkStreaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform {
  def main(args: Array[String]): Unit = {


    /**

    /**
      * 当运行Spark Streaming应用程序的时候如果使用的Local模式，
      * 不要使用local或者local[1]作为master的URL。
      * 因为这种写法意味着仅仅只有一个线程能被使用，
      * 如果使用基于Receiver的input DStream（如果用的HDFS上面的文件就可以用local[1]或local），
      * Receiver就已经占用了线程了，
      * 主流程就处理不了数据了。
      * 所以要用local[n]，n>Receiver的数量。
      * */

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SocketWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

//    构建黑名单
    val blacks = List("lee","leo")
    val blacksRDD = ssc.sparkContext.parallelize(blacks)
      .map(x=>(x,true))

    val lines = ssc.socketTextStream("",9999)

    /**
      *
    访问日志 ==>DStream
    20180718，sid
    20180718，lee
    20180718，leo
      ==>（sid:20180718，sid）（lee:20180718，lee）（leo:20180718，leo）
    leftjoin
    黑名单表 ==>RDD
    lew
    leo
       ==>(lee:true)(leo:true)
    结果
    sid:[<20180718，sid>,<false>]）  ==> tuple 1
    lee:[<20180718，lee>,<true>]）   X
    leo:[<20180718，leo>,<true>]）   X
      */
    val clicklog = lines.map(x => (x.split(",")(1), x))
      .transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

    clicklog.print()
    ssc.start()
    ssc.awaitTermination()

      */


    val conf = new SparkConf().setAppName("transForm").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blacks = List("lee", "leo")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(e => (e, true))

    ssc.socketTextStream("", 9999)
      .map(x => (x.split(" ")(1), 1))
      .transform(rdd => {
        rdd.leftOuterJoin(blacksRDD)
          .filter(x => x._2._2.getOrElse(false) != true)
          .map(x => x._2._2)
      })
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
