package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql.SparkSession

object OneHotEncoderEstimatorExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("OneHotEncoderEstimatorExample")
      .getOrCreate()

    // Note: categorical features are usually first encoded with StringIndexer
    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.show()

    spark.stop()
  }
}
