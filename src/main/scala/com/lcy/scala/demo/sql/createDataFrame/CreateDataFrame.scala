package com.lcy.scala.demo.sql.createDataFrame

import org.apache.spark.sql.SparkSession

object CreateDataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.json("file:/home/ubuntu/sparkData/people.json")

    /**
      * 1
      */
//    df.show()
//    import spark.implicits._
//    df.printSchema()
//    df.select("name").show()
//    df.select($"name", $"age" + 1).show()
//    df.filter($"age" > 21).show()
//    df.groupBy("age").count().show()

    /**
      * 2
      */
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("SELECT * FROM people")
//    sqlDF.show()

    /**
      * 3
      * Global Temporary View
      *
      * Spark SQL中的临时视图是会话范围的，如果创建它的会话终止，它将消失。如果您希望拥有一
      * 个在所有会话之间共享的临时视图并保持活动状态，直到Spark应用程序终止，您可以创建一个
      * 全局临时视图。全局临时视图与系统保留的数据库绑定global_temp，我们必须使用限定名
      * 称来引用它，例如SELECT * FROM global_temp.view1。
      */
//    //将DataFrame注册为全局临时视图
//    df.createGlobalTempView("people")
//    //全局临时视图绑定到系统保留的数据库`global_temp`
//    spark.sql("SELECT * FROM global_temp.people").show()
//    //全局临时视图是跨会话
//    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    /**
      * 4
      */
    import spark.implicits._
    //为case类创建编码器
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(e => println(e)) // Returns: Array(2, 3, 4)
    //可以通过提供类将DataFrame转换为数据集。映射将通过名称
    val peopleDS = spark.read.json("file:/home/ubuntu/sparkData/people.json").as[Person]
    peopleDS.show()

  }

}


case class Person(name: String, age: Long)