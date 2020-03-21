package com.lcy.scala.spark.sql.udf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
 * UDF(User-defined functions, UDFs),即用户自定义函数，在Spark Sql的开发中十分常用，
 * UDF对表中的每一行进行函数处理，返回新的值，有些类似与RDD编程中的Map()算子，实际开发中几乎
 * 每个Spark程序都会使用的。
 */

object UDFDemo {

    def main(args: Array[String]): Unit = {

        /** spark2.0之后版本  */
        val spark = SparkSession
                .builder
                .appName("Demo")
                .master("local")
                .getOrCreate

        val names1 = Array("Leo", "Marry", "Jack", "Tom")
        val nameRowRdd = spark.sparkContext.parallelize(names1, 4)
                .map(name => Row(name))

        val file = StructType(
            Array(StructField("name", StringType, true))
        )

        spark.createDataFrame(nameRowRdd, file).createOrReplaceTempView("names")
        spark.udf.register("length", (str: String) => str.length())

        spark.sql("select name, length(name) as nameLength from names").show()

    }

}


//object UDFDemo2 {
//
//    def main(args: Array[String]): Unit = {
//
//        /** spark1.6版本 */
//        val conf = new SparkConf()
//                .setAppName("UDF")
//                .setMaster("local")
//
//        val sc = new SparkContext(conf)
//        val sqlContext = new SQLContext(sc)
//
//        //构造模拟数据
//        val names = Array("Leo", "Marry", "Jack", "Tom")
//        val namesRDD = sc.parallelize(names, 5)
//        val namesRowRDD = namesRDD.map(name => Row(name))
//        val structType = StructType(
//            Array(StructField("name", StringType, true))
//        )
//        val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)
//
//        //注册一张names表
//        //namesDF.registerTempTable("names")
//        namesDF.createOrReplaceTempView("names")
//        //定义和注册自定义函数
//        //定义函数：自己写匿名函数
//        //注册函数：SQLContext.udf.register()
//        sqlContext.udf.register("strlen", (str: String) => str.length())
//
//        //使用自定义寒素
//        sqlContext.sql("select name,strLen(name) as length from names").show()
//
//    }
//
//}
