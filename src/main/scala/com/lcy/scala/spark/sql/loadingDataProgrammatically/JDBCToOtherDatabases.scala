package com.lcy.scala.spark.sql.loadingDataProgrammatically

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCToOtherDatabases {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkStudy")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/spark?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true"
    val table ="log"
    //注意:JDBC加载和保存可以通过加载/保存或JDBC方法来实现
    //从JDBC源加载数据
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "root")
      .load()


    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    val jdbcDF2 = spark.read
      .jdbc(url, table, properties)
    // 指定读取模式的自定义数据类型
    // 此方法未理解
    properties.put("customSchema", "id DECIMAL(32, 0), content STRING")
    val jdbcDF3 = spark.read
      .jdbc(url, table, properties)


    jdbcDF.show()

    jdbcDF2.show()

    jdbcDF3.show()

//    // 数据保存到JDBC源
    jdbcDF.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "log2")
      .option("user", "root")
      .option("password", "root")
      .save()

    jdbcDF2.write
      .jdbc(url, table, properties)

    // 指定写时创建表列数据类型
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", properties)


  }

}
