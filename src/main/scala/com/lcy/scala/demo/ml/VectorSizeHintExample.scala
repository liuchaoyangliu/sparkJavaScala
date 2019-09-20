package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object VectorSizeHintExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("VectorSizeHint")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataset = spark.createDataFrame(
            Seq(
                (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
                (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
        ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

        val sizeHint = new VectorSizeHint()
                .setInputCol("userFeatures")
                .setHandleInvalid("skip")
                .setSize(3)

        val datasetWithSize = sizeHint.transform(dataset)
        println("过滤掉'userFeatures'不正确大小的行")
        datasetWithSize.show(false)

        val assembler = new VectorAssembler()
                .setInputCols(Array("hour", "mobile", "userFeatures"))
                .setOutputCol("features")

        // 这个数据帧可以像以前一样由下游变压器使用
        val output = assembler.transform(datasetWithSize)
        println("汇总列'小时'，'移动'，'userFeatures'到矢量列'功能'")
//        output.select("features", "clicked").show(false)
        output.show(false)
        spark.stop()

    }

}
