package com.lcy.scala.demo.sql.loadingDataProgrammatically

import org.apache.spark.sql.SparkSession

/** window测试不可以，需要打成.jar包放入Linux环境运行 */

/**
 * spark读取hive中的数据需要将，hadoop中的hive-site.xml，core-size.xml
 * hive中的hdfs-site.xml文件放置在项目的resources目录中
 */

object HiveConnApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("HiveConnApp")
      .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("show databases").show()
    spark.sql("use myhive")
    spark.sql("show tables").show()
    spark.sql("select * from student").show()

  }
}
