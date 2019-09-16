package com.lcy.scala.demo.sql.loadingDataProgrammatically

import org.apache.spark.sql.SparkSession

object JSONDatasets {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStudy")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val peopleDF = spark.read.json("file:\\D:\\sparkData\\people.json")

    peopleDF.printSchema()

    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()

    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()

  }

}
