package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * 标记生成器
 */
object TokenizerExample {


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .master("local")
                .appName("Tokenizer")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val sentenceDataFrame = spark.createDataFrame(Seq(
            (0, "Hi I heard about Spark"),
            (1, "I wish Java could use case classes"),
            (2, "Logistic,regression,models,are,neat")
        )).toDF("id", "sentence")

        val tokenizer = new Tokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")

        val regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W")

        val countTokens = udf { words: Seq[String] => words.length }

        tokenizer
                .transform(sentenceDataFrame)
                .select("sentence", "words")
                .withColumn("tokens", countTokens(col("words")))
                .show(false)

        val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
        regexTokenized
                .select("sentence", "words")
                .withColumn("tokens", countTokens(col("words")))
                .show(false)

        spark.stop()
    }


    //          def main(args: Array[String]): Unit = {
    //
    //              val spark = SparkSession
    //                      .builder
    //                      .master("local")
    //                      .appName("Tokenizer")
    //                      .getOrCreate()
    //              spark.sparkContext.setLogLevel("ERROR")
    //
    //              val sentenceDataFrame = spark.createDataFrame(Seq(
    //                  (0, "Hi I heard about Spark"),
    //                  (1, "I wish Java could use case classes"),
    //                  (2, "Logistic,regression,models,are,neat")
    //              )).toDF("id", "sentence")
    //
    //              /**
    //               * 一个标记生成器，它将输入字符串转换为小写，然后用空格分隔它。
    //               */
    //              val tokenizer = new Tokenizer()
    //                      .setInputCol("sentence")
    //                      .setOutputCol("words")
    //
    //              /**
    //               * 基于正则表达式的标记生成器，通过使用提供的正则表达式模式来分割文本（默认）
    //               * 或重复匹配正则表达式（如果“gap”为假），则提取标记。
    //               * 可选参数还允许使用最小长度过滤令牌。
    //               * 它返回一个可以为空的字符串数组。
    //               */
    //
    //              val regexTokenizer = new RegexTokenizer()
    //                      .setInputCol("sentence")
    //                      .setOutputCol("words")
    //                      .setPattern("\\W")
    //
    //              val countTokens = udf { words: Seq[String] => words.length }
    //
    //              tokenizer
    //                      .transform(sentenceDataFrame)
    //
    //                      /**
    //                       * 选择一组列这是`select`的变体，只能使用列名选择*现有列（即不能构造表达式）。
    //                       */
    //                      .select("sentence", "words")
    //
    //                      /**
    //                       * 通过添加列或替换现有列来返回新的数据集同名。
    //                       *
    //                       * `column`的表达式只能引用此数据集提供的属性。 它是一个
    //                       * 添加引用某些其他数据集的列时出错。
    //                       *
    //                       */
    //                      .withColumn("tokens", countTokens(col("words")))
    //                      .show(false)
    //
    //              val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    //              regexTokenized
    //                      .select("sentence", "words")
    //                      .withColumn("tokens", countTokens(col("words")))
    //                      .show(false)
    //
    //              spark.stop()
    //          }

}
