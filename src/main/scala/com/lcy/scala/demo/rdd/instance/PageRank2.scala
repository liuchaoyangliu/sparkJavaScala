package com.lcy.scala.demo.rdd.instance

import org.apache.spark.sql.SparkSession

object PageRank2 {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .master("local")
      .getOrCreate()

    val iters =  10
    val lines = spark.read.textFile("file:/D:/sparkData/page.txt").rdd

    val links = lines.map{ s =>
      val parts = s.split(" ")
      (parts(0), parts(1))
    }
      .distinct()
      .groupByKey()
      .cache()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    spark.stop()
  }
}

