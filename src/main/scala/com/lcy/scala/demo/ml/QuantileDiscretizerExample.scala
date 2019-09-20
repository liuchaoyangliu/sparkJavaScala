package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

object QuantileDiscretizerExample {
    def main(args: Array[String]) {
        val spark = SparkSession
                .builder
                .appName("QuantileDiscretizerExample")
                .getOrCreate()

        val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
        val df = spark.createDataFrame(data).toDF("id", "hour")
                // $example off$
                // Output of QuantileDiscretizer for such small datasets can depend on the number of
                // partitions. Here we force a single partition to ensure consistent results.
                // Note this is not necessary for normal use cases
                .repartition(1)

        val discretizer = new QuantileDiscretizer()
                .setInputCol("hour")
                .setOutputCol("result")
                .setNumBuckets(3)

        val result = discretizer.fit(df).transform(df)
        result.show(false)

        spark.stop()
    }
}
