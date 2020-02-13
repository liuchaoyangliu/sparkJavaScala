package com.lcy.scala.spark.rdd.broadcastAccumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark中因为算子中的真正逻辑是发送到Executor中去运行的，所以当Executor中需要引用外部变量时，
 * 需要使用广播变量。
 * 累机器相当于统筹大变量，常用于计数，统计。
 *
 * 注意事项
 * 1、能不能将一个RDD使用广播变量广播出去？
 * 不能，因为RDD是不存储数据的。可以将RDD的结果广播出去。
 * 2、广播变量只能在Driver端定义，不能在Executor端定义。
 * 3、在Driver端可以修改广播变量的值，在Executor端无法修改广播变量的值。
 * 4、如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本。
 * 5、如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本。
 */
object Broadcast {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("study").setMaster("local")
        val sc = new SparkContext(conf)

        val word = "class1"
        val broadcast = sc.broadcast(word)
        sc.textFile("file:\\D:\\sparkData\\words.txt")
                .filter(x => x.contains(broadcast.value))
                .foreach(println)
        sc.stop()

    }

}


/**
 * class1 90
 * class2 56
 * class1 87
 * class1 76
 * class2 88
 * class1 95
 * class1 74
 * class2 87
 * class2 67
 * class2 77
 */
