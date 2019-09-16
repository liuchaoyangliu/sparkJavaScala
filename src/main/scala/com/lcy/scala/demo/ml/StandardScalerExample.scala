package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

object StandardScalerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StandardScalerExample")
      .getOrCreate()

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()

    spark.stop()
  }
}
