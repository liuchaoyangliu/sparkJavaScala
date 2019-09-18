package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession

object BinarizerExample {
    
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
                .builder
                .appName("Binarizer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
        val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
        
        val binarizer: Binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5)
        
        val binarizedDataFrame = binarizer.transform(dataFrame)
        
        println(s"具有阈值的二进制化器输出 = ${binarizer.getThreshold}")
        binarizedDataFrame.show()
        
        spark.stop()
        
    }
    
}
