package com.lcy.scala.demo.sql.rowNumberWindowFun

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object RowNumberWindow {

  //开窗函数
  def main(args:Array[String]) {

        /** spark1.6版本 */
        val conf = new SparkConf()
        conf.setAppName("RowNumberWindowFunction")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
    //    hiveContext.sql("use spark")
        hiveContext.sql("drop table if exists sales")
        hiveContext.sql("create table if not exists sales (riqi string,leibie string,jine Int) "
          + "row format delimited fields terminated by ','")
        hiveContext.sql("load data local inpath '/home/ubuntu/demo.txt' into table sales")

        val result = hiveContext.sql(
          "select riqi,leibie,jine  from ( select riqi,leibie,jine, row_number() over (partition by leibie order by jine desc) rank from sales) t "
            + "where t.rank<=3")
        result.show()
  }

}
