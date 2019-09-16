package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

object StopWordsRemoverExample {
    
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
                .builder
                .appName("StopWordsRemoverExample")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val remover = new StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered")
        
        val dataSet = spark.createDataFrame(Seq(
            (0, Seq("I", "saw", "the", "red", "balloon")),
            (1, Seq("Mary", "had", "a", "little", "lamb"))
        )).toDF("id", "raw")
        
        remover.transform(dataSet).show(false)
        
        spark.stop()
        
    }
    
    
}
