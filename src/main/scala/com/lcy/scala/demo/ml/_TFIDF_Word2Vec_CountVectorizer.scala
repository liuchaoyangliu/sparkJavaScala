package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}

object _TFIDF_Word2Vec_CountVectorizer {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("tfidf_test")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)
        sc.setLogLevel("ERROR")

        //Scala默认会导入scala.collection.immutable.Vector，
        // 所以必须显式导入org.apache.spark.mllib.linalg.Vector才能使用MLlib才能使用MLlib提供的Vector。

        //密集向量
        val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
        println(dv)
        //稀疏向量,3表示此向量的长度，第一个Array(0,2)表示的索引，第二个Array(1.0, 3.0)与前面的Array(0,2)是相互对应的，表示第0个位置的值为1.0，第2个位置的值为3
        val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
        println(sv1)
        //稀疏向量, 3表示此向量的长度,Seq里面每一对都是(索引，值）的形式
        val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
        println(sv2)

        //标记点
        val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

        val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))


        //创建矩阵,3行2列
        val dm: Matrix = Matrices.dense(2, 3, Array(1, 0, 2.0, 3.0, 4.0, 5.0))
        println("========dm========")
        println(dm)

        val v0 = Vectors.dense(1.0, 0.0, 3.0)
        val v1 = Vectors.sparse(3, Array(1), Array(2.5))
        val v2 = Vectors.sparse(3, Seq((0, 1.5), (1, 1.8)))

        val rows = sc.parallelize(Seq(v0, v1, v2))
        println("=========rows=======")
        println(rows.collect().toBuffer)

        val mat: RowMatrix = new RowMatrix(rows)


        val seriesX: RDD[Double] = sc.parallelize(List(1.0, 2.0, 3.0)) //a series
        val seriesY: RDD[Double] = sc.parallelize(List(4.0, 5.0, 6.0)) //和seriesX必须有相同的分区和基数
        val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
        val data: RDD[Vector] = rows //每个向量必须是行，不能是列
        val correlMatrix: Matrix = Statistics.corr(data, "pearson")
        println("========correlMatrix========")
        println(correlMatrix)

        val summary: MultivariateStatisticalSummary = Statistics.colStats(rows)
        println("===================")
        println(summary.mean) //每个列值组成的密集向量
        println(summary.variance) //列向量方差
        println(summary.numNonzeros) //每个列的非零值个数

        /**
         * Word2Vec
         */

        val documentDF = sqlContext.createDataFrame(Seq(
            "Hi I heard about Spark".split(" "),
            "I wish Java could use case classes".split(" "),
            "Logistic regression models are neat".split(" ")
        ).map(Tuple1.apply)).toDF("text")

        // Learn a mapping from words to Vectors.
        val word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)
                .setMinCount(0)
        val model = word2Vec.fit(documentDF)
        val result = model.transform(documentDF)
        println("=======word2vec=========")
        result.show(10, false)


        /**
         * Countvectorizer
         */

        val df = sqlContext.createDataFrame(Seq(
            (0, Array("a", "b", "c")),
            (1, Array("a", "b", "b", "c", "a"))
        )).toDF("id", "words")

        // fit a CountVectorizerModel from the corpus
        val cvModel: CountVectorizerModel = new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("features")
                .setVocabSize(3)
                .setMinDF(2)
                .fit(df)

        // alternatively, define CountVectorizerModel with a-priori vocabulary
        val cvm = new CountVectorizerModel(Array("a", "b", "c"))
                .setInputCol("words")
                .setOutputCol("features")
        println("=======CountVectorizerModel=========")
        cvModel.transform(df).show(10, false)


        /**
         * TF-IDF
         */

        val sentenceData = sqlContext.createDataFrame(Seq(
            (0, "Hi I heard about Spark"),
            (0, "I wish Java could use case classes"),
            (1, "Logistic regression models are neat")
        )).toDF("label", "sentence")

        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData = tokenizer.transform(sentenceData)
        val hashingTF = new HashingTF()
                .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
        val featurizedData = hashingTF.transform(wordsData)
        // CountVectorizer也可获取词频向量

        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featurizedData)
        val rescaledData = idfModel.transform(featurizedData)
        rescaledData.show(10, false)

    }


}
