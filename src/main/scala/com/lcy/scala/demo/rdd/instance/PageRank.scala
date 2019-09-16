package com.lcy.scala.demo.rdd.instance

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkStudy").setMaster("local")
    val sc = new SparkContext(conf)

    val links = sc.parallelize(List(
      ("A",List("A","B","C","D")),
      ("B",List("A","B","C")),
      ("C",List("A","B")),
      ("D",List("A")))
    ).partitionBy(new HashPartitioner(100))
      .persist()

    var ranks = links.mapValues(v=>1.0)

    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap { //flapmap结果("A",(List("B","C"),1))
        case (pageId,(links,rank)) => links.map(dest=>(dest,rank/links.size))
      }
      ranks=contributions.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }

      ranks.sortBy(e => e._2, false).collect().foreach(println)
  }

}
