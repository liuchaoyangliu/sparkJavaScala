package com.lcy.scala.demo.sql.loadingDataProgrammatically

import org.apache.spark.sql.SparkSession

object LoadingData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkStudy")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /**
      * 1
      */
//    // Encoders for most common types are automatically provided by importing spark.implicits._
//    import spark.implicits._
//
//    val peopleDF = spark.read.json("file:\\E:\\sparkData\\people.json")
//
//    // DataFrames可以保存为Parquet文件，维护架构信息
//    peopleDF.write.parquet("file:\\E:\\sparkData\\parquet")
//
//    val parquetFileDF = spark.read.parquet("file:\\E:\\sparkData\\parquet")
//
//    // Parquet文件还可以用来创建临时视图，然后在SQL语句中使用
//    parquetFileDF.createOrReplaceTempView("parquetFile")
//    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
//    namesDF.map(attributes => "Name: " + attributes(0)).show()
//    // +------------+
//    // |       value|
//    // +------------+
//    // |Name: Justin|
    // +------------+


    /**
      * 2
      *
      * 由于模式合并是一项相对昂贵的操作，并且在大多数情况下不是必需的，因此我们默认从1.5.0开始关闭它。您可以启用它
      * 数据源选项设置mergeSchema到true读取镶木文件时（如下面的实施例），或
      * 将全局SQL选项设置spark.sql.parquet.mergeSchema为true。
      */

    import spark.implicits._

    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("file:\\E:\\sparkData\\data\\test_table\\key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("file:\\E:\\sparkData\\data\\test_table\\key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("file:\\E:\\sparkData\\data\\test_table")
    mergedDF.printSchema()
    mergedDF.show()

  }

}

