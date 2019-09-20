package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，
 * 它可以计算每个单词在其给定语料库环境下的 分布式词向量（Distributed Representation，亦直接被称为词向量）。
 * 词向量表示可以在一定程度上刻画每个单词的语义。
 */


object Word2VecExample {

    def main(args: Array[String]) {

        val spark = SparkSession
                .builder
                .appName("Word2Vec example")
                .master("local[*]")
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        // 输入数据：每行是一个句子或文档中的单词。
        //首先用一组文档，其中一个词语序列代表一个文档。对于每一个文档，我们将其转换为一个特征向量。
        //此特征向量可以被传递到一个学习算法。
        val documentDF = spark.createDataFrame(Seq(
            "Hi I heard about Spark".split(" "),
            "I wish Java could use case classes".split(" "),
            "Logistic regression models are neat".split(" ")
        ).map(Tuple1.apply))
                .toDF("text")

        // 学习从单词到向量的映射。
        val word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                //要从单词转换的代码的维度。
                .setVectorSize(3)
                //令牌必须看起来包含在word2vec模型的词汇表中的最小次数。
                .setMinCount(0)
        val model = word2Vec.fit(documentDF)

        val result = model.transform(documentDF)
        result.collect().foreach {
            case Row(text: Seq[_], features: Vector) =>
                println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
        }

        spark.stop()

    }

}
