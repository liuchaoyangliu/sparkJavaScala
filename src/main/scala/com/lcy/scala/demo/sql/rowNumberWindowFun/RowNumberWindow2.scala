package com.lcy.scala.demo.sql.rowNumberWindowFun

import org.apache.spark.sql.SparkSession

object RowNumberWindow2 {

  def main(args: Array[String]): Unit = {
    /** spark2.0版本 */
    val spark = SparkSession
      .builder
      .appName("sparkStudy")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("drop table if exists sales")
    spark.sql(
      "create table if not exists sales (riqi string,leibie string,jine Int) row format delimited fields terminated by ' '")
    spark.sql("load data local inpath '/home/ubuntu/demo.txt' into table sales")
    spark.sql("select riqi,leibie,jine  from ( select riqi,leibie,jine, row_number() over (partition by leibie order by jine desc) rank from sales) t "
      + "where t.rank<=2").show()


    // row_number()开窗函数的语法说明
    // 首先可以，在SELECT查询时，使用row_number()函数
    // 其次，row_number()函数后面先跟上OVER关键字
    // 然后括号中，是PARTITION BY，也就是说根据哪个字段进行分组
    // 其次是可以用ORDER BY 进行组内排序
    // 然后row_number()就可以给每个组内的行，一个组内行号


    //    /** 以下代码与上不可用*/
    //    spark.sql("SELECT product, category,revenue FROM ("
    //      + "SELECT product, category, revenue, "
    //      + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank FROM sales "
    //      + ") tmp_sales "
    //      + "WHERE rank<=2").show()

  }

}
