package com.lcy.scala.demo.ml

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
 *
 * 对称的，IndexToString的作用是把标签索引的一列重新映射回原有的字符型标签。
 * 一般都是和StringIndexer配合，先用StringIndexer转化成标签索引，进行模型训练，
 * 然后在预测标签的时候再把标签索引转化成原有的字符标签。当然，也允许你使用自己提供的标签。
 *
 */

object IndexToStringExample {

    /**
     *
     * 我们首先用StringIndexer读取数据集中的“category”列，
     * 把字符型标签转化成标签索引，然后输出到“categoryIndex”列上。
     * 然后再用IndexToString读取“categoryIndex”上的标签索引，
     * 获得原有数据集的字符型标签，然后再输出到“originalCategory”列上。
     * 最后，通过输出“originalCategory”列，可以看到数据集中原有的字符标签。
     *
     */

    def main(args: Array[String]) {

        val spark = SparkSession
                .builder
                .appName("IndexToString")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.createDataFrame(Seq(
            (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")
        )).toDF("id", "category")

        val indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df)
        val indexed = indexer.transform(df)

        println(s"转换字符串列'${indexer.getInputCol}' " + s"索引列 '${indexer.getOutputCol}'")
        indexed.show()

        val inputColSchema = indexed.schema(indexer.getOutputCol)
        println(s"StringIndexer将标签存储在输出列元数据中: " +
                s"${Attribute.fromStructField(inputColSchema).toString}\n")

        val converter = new IndexToString()
                .setInputCol("categoryIndex")
                .setOutputCol("originalCategory")

        val converted = converter.transform(indexed)

        println(s"转换索引列 '${converter.getInputCol}' 回到原始字符串 " +
                s"column '${converter.getOutputCol}' 在元数据中使用标签")
        converted.select("id", "categoryIndex", "originalCategory").show()

        spark.stop()

    }

}
