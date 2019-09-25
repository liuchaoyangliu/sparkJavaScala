package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
 * n-gram是一个整数n的n个标记（通常是单词）的序列。 NGram类可用于将输入要素转换为n-gram。
 * NGram将字符串序列作为输入（例如Tokenizer的输出）。参数n用于确定每个n-gram中的项数。
 * 输出将由一系列n-gram组成，其中每个n-gram由n个连续单词的以空格分隔的字符串表示。如果输入
 * 序列包含少于n个字符串，则不会产生输出。
 *
 */

object NGramExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("NGram")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val wordDataFrame = spark.createDataFrame(Seq(
            (0, Array("Hi", "I", "heard", "about", "Spark")),
            (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
            (2, Array("Logistic", "regression", "models", "are", "neat"))
        )).toDF("id", "words")

        val ngram = new NGram()
                .setN(2)
                .setInputCol("words")
                .setOutputCol("nGrams")

        val ngramDataFrame = ngram.transform(wordDataFrame)
        //        ngramDataFrame.select("nGrams").show(false)
        ngramDataFrame.show(false)
//        +---+------------------------------------------+------------------------------------------------------------------+
//        |id |words                                     |nGrams                                                            |
//        +---+------------------------------------------+------------------------------------------------------------------+
//        |0  |[Hi, I, heard, about, Spark]              |[Hi I, I heard, heard about, about Spark]                         |
//                |1  |[I, wish, Java, could, use, case, classes]|[I wish, wish Java, Java could, could use, use case, case classes]|
//        |2  |[Logistic, regression, models, are, neat] |[Logistic regression, regression models, models are, are neat]    |
//        +---+------------------------------------------+------------------------------------------------------------------+

        spark.stop()
    }
}
