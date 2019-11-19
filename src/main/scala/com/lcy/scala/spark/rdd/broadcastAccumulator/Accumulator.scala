package com.lcy.scala.spark.rdd.broadcastAccumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * (1)通过在driver中调用 SparkContext.accumulator(initialValue) 方法，创建出存有初始值的累加器。
  * 返回值为 org.apache.spark.Accumulator[T] 对象，其中 T 是初始值initialValue 的类型。
  *
  * (2)Spark闭包（函数序列化）里的excutor代码可以使用累加器的 += 方法（在Java中是 add ）增加累加器的值。
  *
  * (3)driver程序可以调用累加器的 value 属性（在 Java 中使用 value() 或 setValue() ）来访问累加器的值。
  *
  */

object Accumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("accumulator")
    val sc = new SparkContext(conf)
    val accumulator = sc.longAccumulator("accumulatorDemo")

    sc.textFile("file:\\D:\\sparkData\\data.txt",2)
      .foreach(//两个变量
      x =>accumulator.add(1))

    println(accumulator.value)
    println(accumulator.name)

//    val accum = sc.longAccumulator("My Accumulator")
//    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
//    println(accum.sum)
//    println(accum.avg)

  }

}
