package com.lcy.scala.demo.sql.createDataFrame

import org.apache.spark.sql.SparkSession

object ReadData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val usersDF = spark.read.load("file:/home/ubuntu/sparkData/parquet")
//    usersDF.select("Id", "CreatedTime").write.save("file:/home/ubuntu/sparkData/parquet")
//    usersDF.select("Id", "CreatedTime").show()
//    spark.read.load("file:/home/ubuntu/sparkData/parquet").select("*").show()

    //读取指定类型的文件
//    val peopleDF = spark.read.format("json").load("file:/home/ubuntu/sparkData/people.json")
//    peopleDF.select("name", "age")
//      .write.format("parquet")
//      .save("file:/home/ubuntu/sparkData/parquet")

//    //可以直接使用SQL查询该文件，而不是使用读取API将文件加载到DataFrame并进行查询
//    spark.sql("SELECT * FROM parquet.`file:/home/ubuntu/sparkData/parquet`").show()

    //    // 读取csv文件
    //    val peopleDFCsv = spark.read.format("csv")
    //      .option("sep", ";")
    //      .option("inferSchema", "true")
    //      .option("header", "true")
    //      .load("examples/src/main/resources/people.csv")

  }

}

