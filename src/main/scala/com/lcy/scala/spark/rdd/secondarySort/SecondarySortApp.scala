package com.lcy.scala.spark.rdd.secondarySort

import org.apache.spark.{SparkConf, SparkContext}

//二次排序
object SecondarySortApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")

    val sc = new SparkContext(conf)

    sc.textFile("file:\\D:\\sparkData\\demo3.txt")
      .map(line =>{
        val str = line.split(" ")
        (new MySecondSort(str(1).trim.toInt, str(0).trim), line)
      })
      .sortByKey()
      .map(pair => pair._2)
      .foreach(println)
  }

}


/**
  * 数据
  * a 2
  * a 2
  * a 3
  * b 3
  * b 4
  * b 4
  */
