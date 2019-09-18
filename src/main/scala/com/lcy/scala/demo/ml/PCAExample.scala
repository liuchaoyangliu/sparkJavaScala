package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 *
 * 主成分分析（PCA） 是一种对数据进行旋转变换的统计学方法，其本质是在线性空间中进行一个基变换，
 * 使得变换后的数据投影在一组新的“坐标轴”上的方差最大化，随后，裁剪掉变换后方差很小的“坐标轴”，
 * 剩下的新“坐标轴”即被称为 主成分（Principal Component） ，它们可以在一个较低维度的子
 * 空间中尽可能地表示原有数据的性质。主成分分析被广泛应用在各种统计学、机器学习问题中，
 * 是最常见的降维方法之一。PCA有许多具体的实现方法，可以通过计算协方差矩阵，甚至是通过上
 * 文提到的SVD分解来进行PCA变换。
 *
 */


object PCAExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("PCAExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val data = Array(
            Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
            Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
            Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
        )

        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

        val pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(3)
                .fit(df)

        val result = pca.transform(df).select("pcaFeatures")
        result.show(false)

        spark.stop()

    }

}
