package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

object VectorIndexerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("VectorIndexer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val data = spark
                .read
                .format("libsvm")
                .load("file:\\D:\\sparkData\\sample_libsvm_data.txt")

        data.show(false)

        val indexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexed")
                .setMaxCategories(10)

        val indexerModel = indexer.fit(data)

        val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
        println(s"Chose ${categoricalFeatures.size} " +
                s"\n 分类特征: ${categoricalFeatures.mkString(", ")}")

        // 使用转换为索引的分类值创建“索引”的新列
        val indexedData = indexerModel.transform(data)
        indexedData.show(false)

        spark.stop()
    }
}
