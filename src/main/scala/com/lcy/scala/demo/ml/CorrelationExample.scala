package com.lcy.scala.demo.ml

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 关联
 *
 * 结果
 * 相关矩阵也叫相关系数矩阵，是由矩阵各列间的相关系数构成的。也就是说，相关矩阵第i行第j列
 * 的元素是原矩阵第i列和第j列的相关系数。
 *
 */

object CorrelationExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("CorrelationExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val data = Seq(
            Vectors.dense(1.0, 0.0, 0.0, -2.0),
            Vectors.dense(4.0, 5.0, 0.0, 3.0),
            Vectors.dense(6.0, 7.0, 0.0, 8.0),
            Vectors.dense(9.0, 0.0, 0.0, 1.0)
        )


        //    val data = Seq(
        //      Vectors.dense(149.0,150.0,153.0,155.0,160.0,155.0,160.0,150.0),
        //      Vectors.dense(81.0,88.0,87.0,99.0,91.0,89.0,95.0,90.0)
        //    )

        val df = data.map(Tuple1.apply).toDF("features")
        df.show(false)
        val Row(coeff1: Matrix) = Correlation.corr(df, "features").head()
        println(s"皮尔逊相关矩阵:\n $coeff1")

        println("\n\n\n")

        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        println(s"斯皮尔曼相关矩阵:\n $coeff2")

        spark.stop()

    }

}
