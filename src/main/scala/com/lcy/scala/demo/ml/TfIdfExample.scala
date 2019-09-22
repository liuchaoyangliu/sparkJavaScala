package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
 * 特征提取器
 * TF-IDF
 * 术语频率 - 逆文档频率（TF-IDF）
 * “词频－逆向文件频率”（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，
 * 它可以体现一个文档中词语在语料库中的重要程度。
 */

object TfIdfExample {

    /**
     * 以一组句子开始。首先使用分解器Tokenizer把句子划分为单个词语。
     * 对每一个句子（词袋），我们使用HashingTF将句子转换为特征向量，
     * 最后使用IDF重新调整特征向量。这种转换通常可以提高使用文本特征的性能。
     */

    //    def main(args: Array[String]) {
    //
    //        val spark = SparkSession
    //                .builder
    //                .appName("TfIdfExample")
    //                .master("local[*]")
    //                .getOrCreate()
    //        spark.sparkContext.setLogLevel("ERROR")
    //
    //        val sentenceData = spark.createDataFrame(Seq(
    //            (0.0, "Hi I heard about Spark"),
    //            (0.0, "I wish Java could use case classes"),
    //            (1.0, "Logistic regression models are neat")
    //        )).toDF("label", "sentence")
    //
    //        val tokenizer = new Tokenizer()
    //                .setInputCol("sentence")
    //                .setOutputCol("words")
    //        val wordsData = tokenizer.transform(sentenceData)
    //
    //        val hashingTF = new HashingTF()
    //                .setInputCol("words")
    //                .setOutputCol("rawFeatures")
    //                .setNumFeatures(20)
    //
    //        val featurizedData = hashingTF.transform(wordsData)
    //
    //        val idf = new IDF()
    //                .setInputCol("rawFeatures")
    //                .setOutputCol("features")
    //        val idfModel = idf.fit(featurizedData)
    //
    //        val rescaledData = idfModel.transform(featurizedData)
    //
    //        rescaledData.select("label", "features").show(false)
    //
    //        spark.stop()
    //    }


    def main(args: Array[String]) {

        val spark = SparkSession
                .builder
                .appName("TfIdfExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        //创建一个集合，每一个句子代表一个文件。
        val sentenceData = spark.createDataFrame(Seq(
            (0.0, "I heard about Spark and I love Spark"),
            (0.0, "I wish Java could use case classes"),
            (1.0, "Logistic regression models are neat")
        )).toDF("label", "sentence")

        //用tokenizer把每个句子分解成单词
        val tokenizer = new Tokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
        //tokenizer的transform（）方法把每个句子拆分成了一个个单词。
        val wordsData = tokenizer.transform(sentenceData)
        //        wordsData.show(false)

        //用HashingTF的transform（）方法把句子哈希成特征向量。我们这里设置哈希表的桶数为2000。
        val hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                //功能数量,应大于0.（默认值= 2 ^ 18 ^）
                .setNumFeatures(2000)

        //每一个单词被哈希成了一个不同的索引值。
        //以”I heard about Spark and I love Spark”为例，输出结果中2000代表哈希表的桶数，
        //“[105,365,727,1469,1858,1926]”分别代表着“i, spark, heard, about, and, love”的哈希值，
        //“[2.0,2.0,1.0,1.0,1.0,1.0]”为对应单词的出现次数。
        val featurizedData = hashingTF.transform(wordsData)
        // 或者，CountVectorizer也可用于获得术语频率向量
        //featurizedData.show(false)
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+
        //|label|sentence                            |words                                        |rawFeatures                                                          |
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+
        //|0.0  |I heard about Spark and I love Spark|[i, heard, about, spark, and, i, love, spark]|(2000,[240,333,1105,1329,1357,1777],[1.0,1.0,2.0,2.0,1.0,1.0])       |
        //|0.0  |I wish Java could use case classes  |[i, wish, java, could, use, case, classes]   |(2000,[213,342,489,495,1329,1809,1967],[1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
        //|1.0  |Logistic regression models are neat |[logistic, regression, models, are, neat]    |(2000,[286,695,1138,1193,1604],[1.0,1.0,1.0,1.0,1.0])                |
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+


        //给定一组文档计算逆文档频率（IDF）。
        //调用IDF方法来重新构造特征向量的规模，生成的idf是一个Estimator，
        //在特征向量上应用它的fit（）方法，会产生一个IDFModel。
        val idf = new IDF()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
        val idfModel = idf.fit(featurizedData)

        //调用IDFModel的transform方法，可以得到每一个单词对应的TF-IDF 度量值。
        val rescaledData = idfModel.transform(featurizedData)

        rescaledData.show(false)
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        //|label|sentence                            |words                                        |rawFeatures                                                          |features                                                                                                                                                                       |
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        //|0.0  |I heard about Spark and I love Spark|[i, heard, about, spark, and, i, love, spark]|(2000,[240,333,1105,1329,1357,1777],[1.0,1.0,2.0,2.0,1.0,1.0])       |(2000,[240,333,1105,1329,1357,1777],[0.6931471805599453,0.6931471805599453,1.3862943611198906,0.5753641449035617,0.6931471805599453,0.6931471805599453])                       |
        //|0.0  |I wish Java could use case classes  |[i, wish, java, could, use, case, classes]   |(2000,[213,342,489,495,1329,1809,1967],[1.0,1.0,1.0,1.0,1.0,1.0,1.0])|(2000,[213,342,489,495,1329,1809,1967],[0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.28768207245178085,0.6931471805599453,0.6931471805599453])|
        //|1.0  |Logistic regression models are neat |[logistic, regression, models, are, neat]    |(2000,[286,695,1138,1193,1604],[1.0,1.0,1.0,1.0,1.0])                |(2000,[286,695,1138,1193,1604],[0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453,0.6931471805599453])                                               |
        //+-----+------------------------------------+---------------------------------------------+---------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

        spark.stop()

    }

}
