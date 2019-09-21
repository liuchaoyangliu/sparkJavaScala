package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 *
 * ChiSqSelector代表Chi-Squared特征选择。它对具有分类特征的标记数据进行操作。
 * ChiSqSelector使用 卡方独立性检验来决定选择哪些功能。它支持五种选择方法：
 * numTopFeatures，percentile，fpr，fdr，fwe：
 *
 * 1.numTopFeatures根据卡方检验选择固定数量的顶部特征。这类似于产生具有最大预测能力的特征。
 * 2.percentile 类似于 numTopFeatures但选择所有功能的一部分而不是固定数量。
 * 3.fpr 选择p值低于阈值的所有特征，从而控制选择的误报率。
 * 4.fdr使用Benjamini-Hochberg过程选择错误发现率低于阈值的所有特征。
 * 5.fwe选择p值低于阈值的所有特征。阈值按1 / numFeatures缩放，从而控制选择的家庭错误率。
 *      默认情况下，选择方法为numTopFeatures，将主要功能的默认数量设置为50。
 *      用户可以使用选择选择方法setSelectorType。
 *
 */

object ChiSqSelectorExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("ChiSqSelector")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val data = Seq(
            (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
            (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
            (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
        )

        val df = spark.createDataset(data).toDF("id", "features", "clicked")

        df.show()

        val selector = new ChiSqSelector()
                .setNumTopFeatures(2)
                .setFeaturesCol("features")
                .setLabelCol("clicked")
                .setOutputCol("selectedFeatures")

        val result = selector.fit(df).transform(df)

        println(s"带顶部的ChiSqSelector输出 ${selector.getNumTopFeatures} 选择的功能")
        result.show()

        spark.stop()

    }

}
