package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object DCTExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("DCT")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val data = Seq(
            Vectors.dense(0.0, 1.0, -2.0, 3.0),
            Vectors.dense(-1.0, -2.0, -5.0, 1.0),
            Vectors.dense(14.0, -2.0, -5.0, 1.0)
        )

        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

        val dct = new DCT()
                .setInputCol("features")
                .setOutputCol("featuresDCT")
                //指示是执行逆DCT（true）还是转发DCT（false）。
                .setInverse(false)

        dct.transform(df).select("featuresDCT").show(false)

        spark.stop()
    }

}

