package com.lcy.scala.spark.rdd.instance

import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("study").setMaster("local")
        val sc = new SparkContext(conf)

        sc.textFile("file:\\D:\\sparkData\\demo.txt")
                .map(line => {
                    val strings = line.split(",")
                    (strings(0), strings(1).toInt)
                })
                .sortBy(e => e._2, false)
                .groupByKey() //排序倒序
                .map(e => (e._1, e._2.take(3)))
                .foreach(e => {
                    print(e._1 + ":")
                    e._2.foreach(e => print(e + " "))
                    println()
                })
        sc.stop()
    }

}


/**
 * 数据
 *
 * class2,1
 * class3,2
 * class3,11
 * class4,12
 * class2,13
 * class3,14
 * class4,15
 * class3,5
 * class4,6
 * class2,7
 * class4,3
 * class2,4
 * class3,8
 * class4,9
 * class2,10
 *
 */
