package com.lcy.scala.demo.rdd.instance

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object PersisCache {

//  持久化
//  其中cache内部调用了persist方法,persist方法又调用了persist(StorageLevel.MEMORY_ONLY)方法,
//  所以执行cache算子其实就是执行了persist算子且持久化级别为MEMORY_ONLY
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("TestStorgeLevel")
    val sc = new JavaSparkContext(conf)

    val text = sc.textFile("file:\\D:\\sparkData\\data2.txt")
    text.persist()

    val startTime1 = System.currentTimeMillis
    val count1 = text.count
    System.out.println(count1)
    val endTime1 = System.currentTimeMillis
    System.out.println(endTime1 - startTime1)

    val startTime2 = System.currentTimeMillis
    val count2 = text.count
    System.out.println(count2)
    val endTime2 = System.currentTimeMillis
    System.out.println(endTime2 - startTime2)

    val startTime3 = System.currentTimeMillis
    val count3 = text.count
    System.out.println(count3)
    val endTime3 = System.currentTimeMillis
    System.out.println(endTime3 - startTime3)

  }

}
