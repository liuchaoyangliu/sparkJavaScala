package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MinMaxScalerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("MinMaxScalere")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.1, -1.0)),
            (1, Vectors.dense(2.0, 1.1, 1.0)),
            (2, Vectors.dense(3.0, 10.1, 3.0))
        )).toDF("id", "features")

        val scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")

        // 计算摘要统计信息并生成MinMaxScalerModel
        val scalerModel = scaler.fit(dataFrame)

        // rescale each feature to range [min, max].
        val scaledData = scalerModel.transform(dataFrame)
        println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
        scaledData.select("features", "scaledFeatures").show()
        scaledData.show()

        spark.stop()

    }

}
