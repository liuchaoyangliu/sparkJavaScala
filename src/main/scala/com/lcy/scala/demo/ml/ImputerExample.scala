package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating Imputer.
 * Run with:
 * bin/run-example ml.ImputerExample
 */
object ImputerExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                .appName("ImputerExample")
                .getOrCreate()

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

        val model = imputer.fit(df)
        model.transform(df).show()

        spark.stop()
    }
}
