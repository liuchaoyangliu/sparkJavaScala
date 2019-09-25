package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object NormalizerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("Normalizer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.5, -1.0)),
            (1, Vectors.dense(2.0, 1.0, 1.0)),
            (2, Vectors.dense(4.0, 10.0, 2.0))
        )).toDF("id", "features")

        // 使用$ L ^ 1 $ norm标准化每个Vector。
        val normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0)

        val l1NormData = normalizer.transform(dataFrame)
        println("使用L ^ 1范数归一化")
        l1NormData.show()

        // 使用$ L ^ \ infty $ norm标准化每个Vector。
        val lInfNormData = normalizer
                .transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
        println("使用L ^ inf范数归一化")
        lInfNormData.show()

        spark.stop()
    }
}
