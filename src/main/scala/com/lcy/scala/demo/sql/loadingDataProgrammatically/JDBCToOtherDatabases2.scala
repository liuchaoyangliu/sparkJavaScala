package com.lcy.scala.demo.sql.loadingDataProgrammatically

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCToOtherDatabases2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("ConnectionMysql")
      .master("local")
      .getOrCreate

    val url = "jdbc:mysql://localhost:3306/spark?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true"
    val table = "log"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")

    /**需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）*/
    val df = spark.read.jdbc(url, table, properties)
    df.createOrReplaceTempView("log")
    spark.sql("select id, ManagerId, Content from log").show()

    /** 保存数据导数据库 */
    //    df.write.jdbc(url, "log2", properties) // 此方法会创建一个名为log2的表
    /** 以json的格式保存 spark默认的保存文件的方式为 parquet*/
    df.write.format("json").save("file:\\E:\\sparkData\\newLog")

    //    val df2 = spark.sql("select id, ManagerId, Content from log")
    //    df2.write.format("json").save("file:\\E:\\sparkData\\newLog2")

  }


}
