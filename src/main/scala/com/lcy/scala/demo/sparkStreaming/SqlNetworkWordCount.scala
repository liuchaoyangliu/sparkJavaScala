package com.lcy.scala.demo.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlNetworkWordCount {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ssc.socketTextStream("", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    // 将单词DStream的RDDs转换为DataFrame并运行SQL查询
    words.foreachRDD { (rdd: RDD[String], time: Time) =>

      // 获取SparkSession的单例实例
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()

    }

    ssc.start()
    ssc.awaitTermination()
  }
}


/** 用于将RDD转换为数据aframe的Case类 */
case class Record(word: String)


/** 延迟实例化SparkSession的单例实例 */
object SparkSessionSingleton {

  @transient
  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }

}
