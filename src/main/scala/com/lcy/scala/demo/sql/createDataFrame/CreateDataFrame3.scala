package com.lcy.scala.demo.sql.createDataFrame

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateDataFrame3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("demo").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val rdd = spark.sparkContext.textFile("file:/home/ubuntu/sparkData/data.txt")
      .map(line => line.split(","))
      .map(line => Row(line(0)trim, line(1).trim.toInt))

    val table = StructType(
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      ))

    val df = spark.createDataFrame(rdd, table)
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    /** 将数据写入数据库 */
    val url = "jdbc:mysql://localhost:3306/spark?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    df.write.jdbc(url, "people", properties)

    import spark.implicits._
    spark.sql("select * from people")
      .map(row => ("姓名:" + row(0) + " 年龄:" + row(1)))
      .show()

    /** 保存为文本文件 */
    df.rdd.saveAsTextFile("file:/home/ubuntu/sparkData/peopleFile")

    /** 保存为json格式的文件 */
    df.write.json("file:/home/ubuntu/sparkData/peopleJson")

  }

}
