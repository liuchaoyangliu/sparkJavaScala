package com.lcy.scala.demo.ml

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 关联
 */

object CorrelationExample {

  /**

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("CorrelationExample")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

//    val data = Seq(
//      Vectors.dense(149.0,150.0,153.0,155.0,160.0,155.0,160.0,150.0),
//      Vectors.dense(81.0,88.0,87.0,99.0,91.0,89.0,95.0,90.0)
//    )

    val df = data.map(Tuple1.apply).toDF("features")

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head()
    println(s"Pearson correlation matrix:\n $coeff1")

    println()
    println()
    println()
    println()


    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")

    spark.stop()

  }

   */


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("correlation")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply).toDF("features")

    val row = Correlation.corr(df, "features").head()
    println(s"Pearson correlation matix:\n $row")

    println()

    val frame = Correlation.corr(df, "features", "spearman").head()
    println(s"Spearman correlation matrix:\n $frame")

    spark.stop()

  }


}
