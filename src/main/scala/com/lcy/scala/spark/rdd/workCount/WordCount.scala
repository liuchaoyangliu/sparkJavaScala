package com.lcy.scala.spark.rdd.workCount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkStudy").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.textFile("file:D:\\sparkData\\data2.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .foreach(println)
    sc.stop()

  }

}
