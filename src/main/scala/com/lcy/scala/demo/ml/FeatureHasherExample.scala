package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.sql.SparkSession

object FeatureHasherExample {
    
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
                .builder
                .appName("FeatureHasherExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val dataset = spark.createDataFrame(Seq(
            (2.2, true, "1", "foo"),
            (3.3, false, "2", "bar"),
            (4.4, false, "3", "baz"),
            (5.5, false, "4", "foo")
        )).toDF("real", "bool", "stringNum", "string")
        
        val hasher = new FeatureHasher()
                .setInputCols("real", "bool", "stringNum", "string")
                .setOutputCol("features")
        
        val featurized = hasher.transform(dataset)
        
        featurized.show(false)
        
        spark.stop()
        
    }
    
}
