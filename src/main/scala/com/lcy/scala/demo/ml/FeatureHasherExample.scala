package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.SparkSession

/**
 * 特征哈希将一组分类或数字特征投影到指定维的特征向量中（通常大大小于原始特征空间的特征向量）。
 * 这是通过使用哈希技巧 将特征映射到特征向量中的索引来完成的。
 */

object FeatureHasherExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("FeatureHasherExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataset = spark.createDataFrame(Seq(
            (2.2, true, "1", "foo"),
            (3.3, false, "2", "bar"),
            (4.4, false, "3", "baz"),
            (5.5, false, "4", "foo")
        )).toDF("real", "bool", "stringNum", "string")

        val hasher = new FeatureHasher()
                .setInputCols("real", "bool", "stringNum", "string")
                .setOutputCol("features")

        val featurized = hasher.transform(dataset)

        featurized.show(false)

//        +----+-----+---------+------+--------------------------------------------------------+
//        |real|bool |stringNum|string|features                                                |
//        +----+-----+---------+------+--------------------------------------------------------+
//        |2.2 |true |1        |foo   |(262144,[174475,247670,257907,262126],[2.2,1.0,1.0,1.0])|
//        |3.3 |false|2        |bar   |(262144,[70644,89673,173866,174475],[1.0,1.0,1.0,3.3])  |
//        |4.4 |false|3        |baz   |(262144,[22406,70644,174475,187923],[1.0,1.0,4.4,1.0])  |
//        |5.5 |false|4        |foo   |(262144,[70644,101499,174475,257907],[1.0,1.0,5.5,1.0]) |
//        +----+-----+---------+------+--------------------------------------------------------+

        spark.stop()

    }
}
