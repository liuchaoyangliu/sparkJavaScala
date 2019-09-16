package com.lcy.scala.demo.sql.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UDAF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkStudy")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.udf.register("myAverage", MyAverage1)

    val df = spark.read.json("file:\\D:\\sparkData\\employees.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()

  }

}


object MyAverage1 extends UserDefinedAggregateFunction {

  // 数据类型的这个集合函数的输入参数
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  // 聚合缓冲区中的值的数据类型
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // 返回值的数据类型
  def dataType: DataType = DoubleType

  // 此函数是否始终在相同的输入
  def deterministic: Boolean = true

  // 初始化给定的聚合缓冲区。缓冲区本身是一个“行”
  //提供了一些标准方法，比如在索引处检索值(例如get()、getBoolean())
  //更新其值的机会。注意，缓冲区内的数组和映射仍然是不可变的。
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //使用来自“input”的新输入数据更新给定的聚合缓冲区“buffer”
  //  局部累加
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 合并两个聚合缓冲区并将更新后的缓冲区值存储回' buffer1 '
  //  全局累加
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)

}

