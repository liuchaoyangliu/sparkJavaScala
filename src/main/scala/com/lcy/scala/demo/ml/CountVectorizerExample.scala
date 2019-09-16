package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
 *
 * CountVectorizer旨在通过计数来将一个文档转换为向量。当不存在先验字典时，
 * Countvectorizer作为Estimator提取词汇进行训练，并生成一个CountVectorizerModel
 * 用于存储相应的词汇向量空间。该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法，
 *
 *
 * 在CountVectorizerModel的训练过程中，CountVectorizer将根据语料库中的词频排序从高到低进行选择，
 * 词汇表的最大含量由vocabsize超参数来指定，超参数minDF，则指定词汇表中的词语至少要在多少个不同文档中出现。
 *
 */

object CountVectorizerExample {
    
    def main(args: Array[String]) {
        
        val spark = SparkSession
            .builder
            .appName("CountVectorizer")
            .master("local[*]")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val df = spark.createDataFrame(Seq(
            (0, Array("a", "b", "c", "d")),
            (1, Array("a", "b", "b", "c", "a"))
        )).toDF("id", "words")
        
        
        /**
         * 适合语料库中的CountVectorizerModel
         *
         * 通过CountVectorizer设定超参数，训练一个CountVectorizerModel，
         * 这里设定词汇表的最大量为3，设定词汇表中的词至少要在2个文档中出现过，
         * 以过滤那些偶然出现的词汇。
         */
        val cvModel: CountVectorizerModel =
            new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("features")
                
                /**
                 * 词汇量的最大大小。
                 * CountVectorizer将构建一个词汇表，该词汇表仅考虑在语料库中按术语频率排序的顶级
                 * vocabSize术语。
                 * 默认值：2 ^ 18 ^
                 */
                .setVocabSize(3)
                
                /**
                 * 指定术语必须出现在词汇表中的最小数量的不同文档。
                 * 如果这是一个大于或等于1的整数，则指定该术语必须出现的文件数
                 * 如果这是[0,1）中的双精度数，则指定文档的分数。
                 *
                 * 指定词汇表中的词语至少要在多少个不同文档中出现。
                 */
                .setMinDF(2)
                .fit(df)
        
        /**
         * 在训练结束后，可以通过CountVectorizerModel的vocabulary成员获得到模型的词汇表：
         */
//        cvModel.vocabulary.foreach(e => println(e))
        
        
        /**
         * 或者，使用先验词汇表定义CountVectorizerModel
         *
         * CountVectorizerModel可以通过指定一个先验词汇表来直接生成，
         * 如以下例子，直接指定词汇表的成员是“a”，“b”，“c”三个词：
         */
        val cvm = new CountVectorizerModel(Array("a", "b", "c"))
            .setInputCol("words")
            .setOutputCol("features")
        
        cvModel.transform(df).show(false)
        cvm.transform(df).show(false)
        
        spark.stop()
        
    }
    
}
