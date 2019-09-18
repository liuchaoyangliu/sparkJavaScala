package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object PolynomialExpansionExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("PolynomialExpansion")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val data = Array(
            Vectors.dense(2.0, 1.0),
            Vectors.dense(0.0, 0.0),
            Vectors.dense(3.0, -1.0)
        )
        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

        val polyExpansion = new PolynomialExpansion()
                .setInputCol("features")
                .setOutputCol("polyFeatures")
                .setDegree(3)

        val polyDF = polyExpansion.transform(df)
        polyDF.show(false)

        spark.stop()

    }

}
