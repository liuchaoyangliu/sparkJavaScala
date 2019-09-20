package com.lcy.scala.demo.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * 管道
 */

object PipelineExample {


    //  def main(args: Array[String]): Unit = {
    //
    //    val spark = SparkSession
    //      .builder
    //      .master("local[*]")
    //      .appName("PipelineExample")
    //      .getOrCreate()
    //
    //    // 从（id，text，label）元组列表中准备培训文档。
    //    val training = spark.createDataFrame(Seq(
    //      (0L, "a b c d e spark", 1.0),
    //      (1L, "b d", 0.0),
    //      (2L, "spark f g h", 1.0),
    //      (3L, "hadoop mapreduce", 0.0)
    //    )).toDF("id", "text", "label")
    //
    //    // 配置ML管道，包括三个阶段：tokenizer，hashingTF和lr。
    //    val tokenizer = new Tokenizer()
    //      .setInputCol("text")
    //      .setOutputCol("words")
    //    val hashingTF = new HashingTF()
    //      .setNumFeatures(1000)
    //      .setInputCol(tokenizer.getOutputCol)
    //      .setOutputCol("features")
    //    val lr = new LogisticRegression()
    //      .setMaxIter(10)
    //      .setRegParam(0.001)
    //    val pipeline = new Pipeline()
    //      .setStages(Array(tokenizer, hashingTF, lr))
    //
    //    // 使管道适合培训文件。
    //    val model = pipeline.fit(training)
    //
    //    // 现在我们可以选择将安装的管道保存到磁盘
    //    model.write.overwrite().save("/tmp/spark-logistic-regression-model")
    //
    //    // 我们还可以将这个不合适的管道保存到磁盘
    //    pipeline.write.overwrite().save("/tmp/unfit-lr-model")
    //
    //    // 并在生产过程中将其重新加载
    //    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
    //
    //    // 准备测试文档，这些文档是未标记的（id，text）元组。
    //    val test = spark.createDataFrame(Seq(
    //      (4L, "spark i j k"),
    //      (5L, "l m n"),
    //      (6L, "spark hadoop spark"),
    //      (7L, "apache hadoop")
    //    )).toDF("id", "text")
    //
    //    // 对测试文档进行预测。
    //    model.transform(test)
    //      .select("id", "text", "probability", "prediction")
    //      .collect()
    //      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
    //        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    //      }
    //
    //    spark.stop()
    //  }

    //  def main(args: Array[String]): Unit = {
    //
    //    val spark = SparkSession
    //      .builder
    //      .master("local[*]")
    //      .appName("PipelineExample")
    //      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
    //
    //    val training = spark.createDataFrame(Seq(
    //      (0L, "a b c d e spark", 1.0),
    //      (1L, "b d", 0.0),
    //      (2L, "spark f g h", 1.0),
    //      (3L, "hadoop mapreduce", 0.0)
    //    )).toDF("id", "text", "label")
    //
    //    //标记生成器
    //    val tokenizer = new Tokenizer()
    //      .setInputCol("text")
    //      .setOutputCol("words")
    //
    //    val hashingTF = new HashingTF()
    //      .setNumFeatures(1000)
    //      .setInputCol(tokenizer.getOutputCol)
    //      .setOutputCol("features")
    //
    //    //逻辑回归
    //    val lr = new LogisticRegression()
    //      .setMaxIter(10)
    //      .setRegParam(0.001)
    //    val pipeline = new Pipeline()
    //      .setStages(Array(tokenizer, hashingTF, lr))
    //
    //    val model = pipeline.fit(training)
    //
    ////    model.write.overwrite().save("file:/home/ubuntu/sparkData/tmp/spark-logistic-regression-model")
    ////
    ////    pipeline.write.overwrite().save("file:/home/ubuntu/sparkData/tmp/unfit-lr-model")
    ////
    ////    val sameModel = PipelineModel.load("file:/home/ubuntu/sparkData/tmp/spark-logistic-regression-model")
    //
    //    val test = spark.createDataFrame(Seq(
    //      (4L, "spark i j k"),
    //      (5L, "l m n"),
    //      (6L, "spark hadoop spark"),
    //      (7L, "apache hadoop")
    //    )).toDF("id", "text")
    //
    //    model.transform(test)
    //      .select("id", "text", "probability", "prediction")
    //      .collect()
    //      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
    //        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    //      }
    //
    //    spark.stop()
    //  }


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("pipeline")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val training = spark.createDataFrame(Seq(
            (0L, "a b c d e spark", 1.0),
            (1L, "b d", 0.0),
            (2L, "spark f g d", 1.0),
            (3L, "hadoop mapreduce", 0.0)
        ))
                .toDF("id", "text", "label")

        val tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words")

        val hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol)
                .setOutputCol("features")

        val lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001)

        val pipeline = new Pipeline()
                .setStages(Array(tokenizer, hashingTF, lr))

        val model = pipeline.fit(training)


        val test = spark.createDataFrame(Seq(
            (4L, "spark i j k"),
            (5L, "l m n"),
            (6L, "spark hadoop spark"),
            (7L, "spache hadoop")
        ))
                .toDF("id", "text")

        model
                .transform(test)
                .select("id", "text", "probability", "prediction")
                .collect()
                .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
                    println(s"($id, $text) --> prob=$prob, prediction=$prediction")
                }

        spark.stop()


    }

}
