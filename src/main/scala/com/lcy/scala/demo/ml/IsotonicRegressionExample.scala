package com.lcy.scala.demo.ml

import org.apache.spark.ml.regression.IsotonicRegression
import org.apache.spark.sql.SparkSession

object IsotonicRegressionExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder
                .appName(s"${this.getClass.getSimpleName}")
                .getOrCreate()

        // Loads data.
        val dataset = spark.read.format("libsvm")
                .load("data/mllib/sample_isotonic_regression_libsvm_data.txt")

        // Trains an isotonic regression model.
        val ir = new IsotonicRegression()
        val model = ir.fit(dataset)

        println(s"Boundaries in increasing order: ${model.boundaries}\n")
        println(s"Predictions associated with the boundaries: ${model.predictions}\n")

        // Makes predictions.
        model.transform(dataset).show()

        spark.stop()
    }
}
