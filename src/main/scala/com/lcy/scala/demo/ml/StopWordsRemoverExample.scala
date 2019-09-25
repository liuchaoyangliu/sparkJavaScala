package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/**
 *
 * 停用词是应从输入中排除的词，通常是因为这些词频繁出现且含义不大。
 *
 * StopWordsRemover将一个字符串序列（例如Tokenizer的输出）作为输入，
 * 并从输入序列中删除所有停用词。停用词列表由stopWords参数指定。
 * 可以通过调用来访问某些语言的默认停用词StopWordsRemover.loadDefaultStopWords(language)，
 * 其可用选项为“丹麦语”，“荷兰语”，“英语”，“芬兰语”，“法语”，“德语”，“匈牙利语”，“意大利语”，“挪威语” ”，
 * “葡萄牙语”，“俄语”，“西班牙语”，“瑞典语”和“土耳其语”。布尔参数caseSensitive指示匹配是否区分大小写
 * （默认情况下为false）。
 *
 */

object StopWordsRemoverExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("StopWordsRemover")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val remover = new StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered")

        val dataSet = spark.createDataFrame(Seq(
            (0, Seq("I", "saw", "the", "red", "balloon")),
            (1, Seq("Mary", "had", "a", "little", "lamb"))
        )).toDF("id", "raw")

        remover.transform(dataSet).show(false)

        spark.stop()

    }
}
