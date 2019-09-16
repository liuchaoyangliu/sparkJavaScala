package com.lcy.scala.demo.ml

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.SparkSession

/**
 * 假设检验
 */

object ChiSquareTestExample {

    //  def main(args: Array[String]): Unit = {
    //
    //    val spark = SparkSession
    //      .builder
    //      .appName("ChiSquare")
    //      .master("local[*]")
    //      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
    //    import spark.implicits._
    //
    //    val data = Seq(
    //      (0.0, Vectors.dense(0.5, 10.0)),
    //      (0.0, Vectors.dense(1.5, 20.0)),
    //      (1.0, Vectors.dense(1.5, 30.0)),
    //      (0.0, Vectors.dense(3.5, 30.0)),
    //      (0.0, Vectors.dense(3.5, 40.0)),
    //      (1.0, Vectors.dense(3.5, 40.0))
    //    )
    //
    //    val df = data.toDF("label", "features")
    //    val chi = ChiSquareTest.test(df, "features", "label").head
    //
    //    println(s"pValues = ${chi.getAs[Vector](0)}")
    //    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    //    println(s"statistics ${chi.getAs[Vector](2)}")
    //
    //    spark.stop()
    //
    //  }


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("chiSquareTest")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val data = Seq(
            (1.0, Vectors.dense(1.0, 2.0)),
            (2.0, Vectors.dense(2.0, 3.0)),
            (3.0, Vectors.dense(3.0, 4.0)),
            (4.0, Vectors.dense(4.0, 5.0)),
            (5.0, Vectors.dense(5.0, 6.0)),
            (5.0, Vectors.dense(6.0, 7.0)),
            (7.0, Vectors.dense(7.0, 8.0))
        )

        val df = data.toDF("label", "features")
        val chi = ChiSquareTest.test(df, "features", "label").head()

        println(s"pValues = ${chi.getAs[Vector](0)}")
        println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
        println(s"statistics ${chi.getAs[Vector](2)}")

        spark.stop()

    }

}
