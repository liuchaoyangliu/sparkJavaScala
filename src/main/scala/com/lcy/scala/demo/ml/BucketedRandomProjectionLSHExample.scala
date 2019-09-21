package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object BucketedRandomProjectionLSHExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("BucketedRandomProjectionLSH")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dfA = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 1.0)),
            (1, Vectors.dense(1.0, -1.0)),
            (2, Vectors.dense(-1.0, -1.0)),
            (3, Vectors.dense(-1.0, 1.0))
        )).toDF("id", "features")

        val dfB = spark.createDataFrame(Seq(
            (4, Vectors.dense(1.0, 0.0)),
            (5, Vectors.dense(-1.0, 0.0)),
            (6, Vectors.dense(0.0, 1.0)),
            (7, Vectors.dense(0.0, -1.0))
        )).toDF("id", "features")

        val key = Vectors.dense(1.0, 0.0)

        val brp = new BucketedRandomProjectionLSH()
                .setBucketLength(2.0)
                .setNumHashTables(3)
                .setInputCol("features")
                .setOutputCol("hashes")

        val model = brp.fit(dfA)

        //特征转换
        println("散列值存储在“散列”列中的散列数据集：")
        model.transform(dfA).show(false)

        //计算输入行的位置敏感哈希，然后执行近似
        //相似连接。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxSimilarityJoin（transformedA，transformedB，1.5）`
        println("在小于1.5的欧氏距离上近似连接dfA和dfB：")
        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
                .select(
                    col("datasetA.id").alias("idA"),
                    col("datasetB.id").alias("idB"),
                    col("EuclideanDistance")
                ).show(false)

        //计算输入行的位置敏感哈希，然后执行近似最近
        //邻居搜索。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxNearestNeighbors（transformedA，key，2）`
        println("大约在dfA中搜索密钥的2个最近邻居：")
        model.approxNearestNeighbors(dfA, key, 2).show(false)

        spark.stop()

    }

}
