package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object ElementwiseProductExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("ElementwiseProduct")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        // 创建一些矢量数据;也适用于稀疏矢量
        val dataFrame = spark.createDataFrame(Seq(
            ("a", Vectors.dense(1.0, 2.0, 3.0)),
            ("b", Vectors.dense(4.0, 5.0, 6.0)))
        ).toDF("id", "vector")

        val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
        val transformer = new ElementwiseProduct()
                .setScalingVec(transformingVector)
                .setInputCol("vector")
                .setOutputCol("transformedVector")

        // 批量转换矢量以创建新列：
        transformer.transform(dataFrame).show()

        spark.stop()
    }
}
