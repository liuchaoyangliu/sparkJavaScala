package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

object QuantileDiscretizerExample {

    def main(args: Array[String]) {

        val spark = SparkSession
                .builder
                .appName("QuantileDiscretizer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
        val df = spark.createDataFrame(data)
                .toDF("id", "hour")
                //这些小数据集的QuantileDiscretizer输出可能取决于数量
                //分区在这里，我们强制单个分区以确保一致的结果。
                //注意这对于正常用例不是必需的
                .repartition(1)

        val discretizer = new QuantileDiscretizer()
                .setInputCol("hour")
                .setOutputCol("result")
                //数据点分组的桶数（分位数或类别）。必须*大于或等于2。
                .setNumBuckets(3)

        val result = discretizer
                .fit(df)
                .transform(df)
        result.show(false)

        spark.stop()
    }
}
