package com.lcy.scala.spark.sql.udf

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object UDFDemo3 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("SparkStudy")
                .master("local")
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val ds = spark
                .read
                .json("file:\\D:\\sparkData\\employees.json")
                .as[Employee]

        ds.show()

        val averageSalary = MyAverage2.toColumn.name("average_salary")
        val result = ds.select(averageSalary)
        result.show()

    }


    case class Employee(name: String, salary: Long)

    case class Average(var sum: Long, var count: Long)

    /**
     * 类型安全的用户定义聚合函数
     * abstract class Aggregator[-IN, BUF, OUT] extends Serializable {
     * IN 聚合的输入类型。
     * BUF 中间值的类型约简。
     * OUT 最终输出结果的类型。l
     */
    object MyAverage2 extends Aggregator[Employee, Average, Double] {

        // 此聚合的值为零。应该满足任意b + 0 = b的性质
        def zero: Average = Average(0L, 0L)

        //将两个值组合起来生成一个新值。为了提高性能，函数可以修改“buffer”并返回它，而不是构造一个新对象
        def reduce(buffer: Average, employee: Employee): Average = {
            buffer.sum += employee.salary
            buffer.count += 1
            buffer
        }

        // 合并两个中间值
        def merge(b1: Average, b2: Average): Average = {
            b1.sum += b2.sum
            b1.count += b2.count
            b1
        }

        // 转换约简的输出
        def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

        // 指定中间值类型的编码器
        def bufferEncoder: Encoder[Average] = Encoders.product

        // 指定最终输出值类型的编码器
        def outputEncoder: Encoder[Double] = Encoders.scalaDouble

    }


}





/**
 * 数据
 *
 * {"name": "foo", "salary": 20}
 * {"name": "bar", "salary": 24}
 * {"name": "baz", "salary": 18}
 * {"name": "foo1", "salary": 17}
 * {"name": "bar2", "salary": 19}
 * {"name": "baz3", "salary": 20}
 *
 */
