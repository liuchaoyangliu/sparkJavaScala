package com.lcy.scala.demo.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Estimator，Transformer 和 Param
 */

object EstimatorTransformerParamExample {


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .master("local[*]")
                .appName("EstimatorTransformerParamExample")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val training = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(0.0, 1.1, 0.1)),
            (0.0, Vectors.dense(2.0, 1.0, -1.0)),
            (0.0, Vectors.dense(2.0, 1.3, 1.0)),
            (1.0, Vectors.dense(0.0, 1.2, -0.5))
        )).toDF("label", "features")

        val lr = new LogisticRegression()
        //        println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

        lr.setMaxIter(10)
                .setRegParam(0.01)

        val model1 = lr.fit(training)
        //        println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

        val paramMap = ParamMap(lr.maxIter -> 20)
                .put(lr.maxIter, 30)
                .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
        val paramMapCombined = paramMap ++ paramMap2

        val model2 = lr.fit(training, paramMapCombined)
        //        println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

        val test = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
            (0.0, Vectors.dense(3.0, 2.0, -0.1)),
            (1.0, Vectors.dense(0.0, 2.2, -1.5))
        )).toDF("label", "features")

        //        model2.transform(test)
        //                .select("features", "label", "myProbability", "prediction")
        //                .collect()
        //                .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        //                    println(s"($features, $label) -> prob=$prob, prediction=$prediction")
        //                }

        model2.transform(test)
                .collect()
                .foreach(println)

        spark.stop()
    }


    //    def main(args: Array[String]): Unit = {
    //
    //        val spark = SparkSession
    //                .builder
    //                .master("local[*]")
    //                .appName("EstimatorTransformerParamExample")
    //                .getOrCreate()
    //        spark.sparkContext.setLogLevel("ERROR")
    //
    //        //从（标签，功能）元组列表中准备训练数据。
    //        val training = spark.createDataFrame(Seq(
    //            (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    //            (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    //            (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    //            (1.0, Vectors.dense(0.0, 1.2, -0.5))
    //        )).toDF("label", "features")
    //
    //        // 创建LogisticRegression实例。这个实例是一个Estimator。
    //        //逻辑回归算法（LogisticRegression）虽然是线性回归算法，但是其它线性回归有所不同，
    //        // 逻辑回归的预测结果只有两种，即true（1）和false（0）。
    //        // 因此，Logistic regression ( 逻辑回归 ) ，尽管它的名字是回归，
    //        // 是一个用于分类的线性模型而不是用于回归。所以，逻辑回归算法往往适用于数据的分类。
    //        val lr = new LogisticRegression()
    //        // 打印出参数，文档和任何默认值。
    //        println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
    //
    //        lr.setMaxIter(10)
    //                .setRegParam(0.01)
    //
    //        // 学习LogisticRegression模型。这使用存储在lr中的参数
    //        val model1 = lr.fit(training)
    //        //由于model1是Model（即由Estimator生成的Transformer），
    //        // 我们可以查看fit（）期间使用的参数。
    //        // 这将打印参数（name：value）对，其中names是此
    //        // LogisticRegression实例的唯一ID。
    //        println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")
    //
    //        //我们也可以使用ParamMap指定参数，
    //        // 支持多种指定参数的方法。
    //        val paramMap = ParamMap(lr.maxIter -> 20)
    //                .put(lr.maxIter, 30) // Specify 1 Param. This overwrites the original maxIter.
    //                .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.
    //
    //        //也可以组合ParamMaps。
    //        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name.
    //        val paramMapCombined = paramMap ++ paramMap2
    //
    //        // 现在使用paramMapCombined参数学习一个新模型。
    //        // paramMapCombined通过lr.set *方法覆盖之前设置的所有参数。
    //        val model2 = lr.fit(training, paramMapCombined)
    //        println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")
    //
    //        // 准备测试数据。
    //        val test = spark.createDataFrame(Seq(
    //            (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
    //            (0.0, Vectors.dense(3.0, 2.0, -0.1)),
    //            (1.0, Vectors.dense(0.0, 2.2, -1.5))
    //        )).toDF("label", "features")
    //
    //        /**
    //         *
    //         * 使用Transformer.transform（）方法对测试数据进行预测。
    //         *LogisticRegression.transform仅使用“功能”列。请注意，
    //         * 由于我们先前重命名了lr.probabilityCol参数，因此model2.transform（）
    //         * 输出'myProbability'列而不是通常的'probability'列。
    //         *
    //         */
    //
    //        model2.transform(test)
    //                .select("features", "label", "myProbability", "prediction")
    //                .collect()
    //                .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
    //                    println(s"($features, $label) -> prob=$prob, prediction=$prediction")
    //                }
    //
    //        spark.stop()
    //    }

}
