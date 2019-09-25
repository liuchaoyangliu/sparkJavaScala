package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

//贾卡距离的最小哈希
object MinHashLSHExample {

//    def main(args: Array[String]): Unit = {
//        val spark = SparkSession
//                .builder
//                .appName("MinHashLSH")
//                .master("local")
//                .getOrCreate()
//        spark.sparkContext.setLogLevel("ERROR")
//
//        val dfA = spark.createDataFrame(Seq(
//            (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
//            (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
//            (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
//        )).toDF("id", "features")
//
//        val dfB = spark.createDataFrame(Seq(
//            (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
//            (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
//            (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
//        )).toDF("id", "features")
//
//        val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))
//
//        val mh = new MinHashLSH()
//                .setNumHashTables(5)
//                .setInputCol("features")
//                .setOutputCol("hashes")
//
//        val model = mh.fit(dfA)
//
//        //特征转换
//        println("散列值存储在“散列”列中的散列数据集：")
//        model.transform(dfA).show(false)
//
//        //计算输入行的位置敏感哈希，然后执行近似
//        //相似连接。
//        //我们可以通过传入已转换的数据集来避免计算哈希，例如
//        //`model.approxSimilarityJoin（transformedA，transformedB，0.6）`
//        println("在Jaccard距离小于0.6的情况下大约加入dfA和dfB：")
////        model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
////                .select(
////                    col("datasetA.id").alias("idA"),
////                    col("datasetB.id").alias("idB"),
////                    col("JaccardDistance")
////                ).show(false)
//        model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
//                        .show(false)
//
//        //计算输入行的位置敏感哈希，然后执行近似最近
//        //邻居搜索。
//        //我们可以通过传入已转换的数据集来避免计算哈希，例如
//        //`model.approxNearestNeighbors（transformedA，key，2）`
//        //如果没有足够的近似邻居候选者，则可能返回少于2行
//        //找到。
//        println("大约在dfA中搜索密钥的2个最近邻居：")
//        model.approxNearestNeighbors(dfA, key, 2).show(false)
//
//        spark.stop()
//
//    }


    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder
                .appName("MinHashLSH")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dfA = spark.createDataFrame(Seq(
            (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
            (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
            (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
        )).toDF("id", "features")

        val dfB = spark.createDataFrame(Seq(
            (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
            (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
            (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
        )).toDF("id", "features")

        val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

        val mh = new MinHashLSH()
                .setNumHashTables(5)
                .setInputCol("features")
                .setOutputCol("hashes")

        val model = mh.fit(dfA)

        println("散列值存储在“散列”列中的散列数据集：")
        model.transform(dfA).show(false)

        println("在Jaccard距离小于0.6的情况下大约加入dfA和dfB：")

        model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
                .show(false)

        println("大约在dfA中搜索密钥的2个最近邻居：")
        model.approxNearestNeighbors(dfA, key, 2).show(false)

        spark.stop()
    }
}
