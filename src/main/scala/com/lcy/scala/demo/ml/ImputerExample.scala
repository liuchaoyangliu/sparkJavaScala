package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.SparkSession

/**
 *
 * Imputer转换器使用缺失值所在列的平均值或中位数来完成数据集中的缺失值。
 * 输入列应为DoubleType或FloatType。当前，Imputer不支持分类特征，
 * 并且可能为包含分类特征的列创建不正确的值。 Imputer可以通过.setMissingValue（custom_value）
 * 插入非“ NaN”的自定义值。例如，.setMissingValue（0）将归因于所有出现的（0）。
 *
 * 请注意，输入列中的所有空值都将被视为丢失，因此也会被估算。
 *
 */

object ImputerExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                .appName("Imputer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.createDataFrame(Seq(
            (1.0, Double.NaN),
            (2.0, Double.NaN),
            (Double.NaN, 3.0),
            (4.0, 4.0),
            (5.0, 5.0)
        )).toDF("a", "b")

        val imputer = new Imputer()
                .setInputCols(Array("a", "b"))
                .setOutputCols(Array("out_a", "out_b"))
        //      .setMissingValue(0.0)

        val model = imputer.fit(df)
        model.transform(df).show()
        //  +---+---+-----+-----+
        //  |  a|  b|out_a|out_b|
        //  +---+---+-----+-----+
        //  |1.0|NaN|  1.0|  4.0|
        //  |2.0|NaN|  2.0|  4.0|
        //  |NaN|3.0|  3.0|  3.0|
        //  |4.0|4.0|  4.0|  4.0|
        //  |5.0|5.0|  5.0|  5.0|
        //  +---+---+-----+-----+

        spark.stop()
    }
}
