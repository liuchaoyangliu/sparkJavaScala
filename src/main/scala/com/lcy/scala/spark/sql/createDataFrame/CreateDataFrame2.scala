package com.lcy.scala.spark.sql.createDataFrame

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CreateDataFrame2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /**
      * 使用反射推断模式
      */
//    //对于从RDD到DataFrames的隐式转换，
//    import spark.implicits._
//    //从文本文件创建Person对象的RDD，将其转换为Dataframe
//    val peopleDF = spark.sparkContext
//      .textFile("file:/home/ubuntu/sparkData/people.txt")
//      .map(_.split(","))
//      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
//      .toDF()
//
//    peopleDF.createOrReplaceTempView("people")
//    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
//    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
//    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    /**
      * 以编程方式指定架构
      *
      * 1,Row从原始RDD 创建s的RDD;
      * 2,创建由StructType匹配Row步骤1中创建的RDD中的s 结构 表示的模式。
      * 3,Row通过createDataFrame提供的方法将模式应用于s 的RDD SparkSession。
      */
    val peopleRDD = spark.sparkContext.textFile("file:/home/ubuntu/sparkData/people.txt")
    //模式以字符串
    val schemaString = "name,age"
    //根据模式的字符串生成模式
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, true))
    val schema = StructType(fields)
    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
    //应用架构的RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    // SQL可以在使用DataFrames
    peopleDF.createOrReplaceTempView("people")
    val results = spark.sql("SELECT name FROM people")
    import spark.implicits._
    // SQL查询的结果是DataFrames并支持所有正常的RDD操作
    //结果中行的列可以通过字段索引或字段名称
    results.map(attributes => "Name: " + attributes.getString(0)).show()
    spark.sql("SELECT name, age FROM people")
      .map(str => (str.getString(0), str.getString(1)))
      .rdd
      .foreach(e => println(e._1 + " " + e._2))

  }

}

