package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

/**
 * 二进制化是将数字特征阈值化为二进制（0/1）特征的过程。
 *
 * Binarizer采用通用参数inputCol和outputCol，以及threshold 用于二值化。
 * 大于阈值的特征值将二值化为1.0；等于或小于阈值的值二值化为0.0。Vector和Double类型都支持inputCol。
 */

object BinarizerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("Binarizer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataFrame = spark.createDataFrame(
            Array((0, 0.1), (1, 0.8), (2, 0.2))
        ).toDF("id", "feature")

        val binarizer: Binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5)

        val binarizedDataFrame = binarizer.transform(dataFrame)

        println(s"具有阈值的二进制化器输出 = ${binarizer.getThreshold}")
        binarizedDataFrame.show()

        spark.stop()
    }
}
