package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * 假设检验
 */

object ChiSqSelectorExample {

    /**
     * *
     *
     * def main(args: Array[String]) {
     * val spark = SparkSession
     * .builder
     * .master("local[*]")
     * .appName("ChiSqSelectorExample")
     * .getOrCreate()
     *spark.sparkContext.setLogLevel("ERROR")
     * import spark.implicits._
     * *
     * val data = Seq(
     * (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
     * (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
     * (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
     * )
     * *
     * val df = spark.createDataset(data).toDF("id", "features", "clicked")
     * *
     * val selector = new ChiSqSelector()
     * .setNumTopFeatures(1)
     * .setFeaturesCol("features")
     * .setLabelCol("clicked")
     * .setOutputCol("selectedFeatures")
     * *
     * val result = selector.fit(df).transform(df)
     * *
     * println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
     *result.show()
     * *
     *spark.stop()
     * *
     * }
     */


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("ChiSqSelector")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val data = Seq(
            (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
            (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
            (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
        )

        val df = spark.createDataset(data).toDF("id", "features", "clicked")

        val selector = new ChiSqSelector()
                .setNumTopFeatures(1)
                .setFeaturesCol("features")
                .setLabelCol("clicked")
                .setOutputCol("selectedFeatures")

        val result = selector.fit(df).transform(df)

        println(s"ChiSqSelector ouput with top ${selector.getNumTopFeatures} features selected")
        result.show()

        spark.stop()

    }

}
