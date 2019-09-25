package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MaxAbsScalerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("MaxAbsScaler")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.1, -8.0)),
            (1, Vectors.dense(2.0, 1.0, -4.0)),
            (2, Vectors.dense(4.0, 10.0, 8.0))
        )).toDF("id", "features")

        val scaler = new MaxAbsScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")

        // 计算摘要统计信息并生成MaxAbsScalerModel
        val scalerModel = scaler.fit(dataFrame)

        // 将每个要素重新缩放到[-1,1]范围
        val scaledData = scalerModel.transform(dataFrame)
//        scaledData.select("features", "scaledFeatures").show()
        scaledData.show(false)
        spark.stop()

    }
}
