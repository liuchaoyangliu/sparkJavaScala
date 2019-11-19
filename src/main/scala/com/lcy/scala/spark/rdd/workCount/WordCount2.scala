package com.lcy.scala.spark.rdd.workCount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    sc.textFile("file:/home/ubuntu/sparkData/data2.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_+_)
      .sortBy(e => e._2)
      .saveAsTextFile("file:/home/ubuntu/sparkData/newData")
    sc.stop()

  }
}
