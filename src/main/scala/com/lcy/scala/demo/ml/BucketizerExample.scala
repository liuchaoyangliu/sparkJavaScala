package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

object BucketizerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("Bucketizer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

        val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
        val dataFrame = spark
                .createDataFrame(data.map(Tuple1.apply))
                .toDF("features")

        val bucketizer = new Bucketizer()
                .setInputCol("features")
                .setOutputCol("bucketedFeatures")
                .setSplits(splits)

        // 将原始数据转换为其存储桶索引。
        val bucketedData = bucketizer.transform(dataFrame)

        println(s"Bucketizer输出 ${bucketizer.getSplits.length - 1} 桶")
        bucketedData.show()




        val splitsArray = Array(
            Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
            Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

        val data2 = Array(
            (-999.9, -999.9),
            (-0.5, -0.2),
            (-0.3, -0.1),
            (0.0, 0.0),
            (0.2, 0.4),
            (999.9, 999.9))
        val dataFrame2 = spark
                .createDataFrame(data2)
                .toDF("features1", "features2")

        val bucketizer2 = new Bucketizer()
                .setInputCols(Array("features1", "features2"))
                .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
                .setSplitsArray(splitsArray)

        // 将原始数据转换为其存储桶索引。
        val bucketedData2 = bucketizer2.transform(dataFrame2)

        println(s"Bucketizer输出 [" +
                s"${bucketizer2.getSplitsArray(0).length - 1}, " +
                s"${bucketizer2.getSplitsArray(1).length - 1}] 每个输入列的存储桶")
        bucketedData2.show()

        spark.stop()
    }
}
