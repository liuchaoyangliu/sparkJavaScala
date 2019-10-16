package com.lcy.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.Test;

public class RowNumberWindow {

    @Test
    public void RowNumberWindow1(){
        /** spark1.6版本 */

        SparkConf conf = new SparkConf().setAppName("study");
        SparkContext sc = new SparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        hiveContext.sql("");

        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (riqi string,leibie string,jine Int) "
                + "row format delimited fields terminated by ','");
        hiveContext.sql("load data local inpath '/home/ubuntu/demo.txt' into table sales");

        Dataset<Row> rowDataset = hiveContext.sql(
                "select riqi,leibie,jine  from ( select riqi,leibie,jine, row_number() over (partition by leibie order by jine desc) rank from sales) t "
                        + "where t.rank<=3");
        rowDataset.show();

        sc.stop();
    }

    @Test
    public void RowNumberWindow2(){
        SparkSession spark = SparkSession
                .builder()
                .appName("sparkStudy")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("drop table if exists sales");
        spark.sql(
                "create table if not exists sales (riqi string,leibie string,jine Int) row format delimited fields terminated by ' '");
        spark.sql("load data local inpath '/home/ubuntu/demo.txt' into table sales");
        spark.sql("select riqi,leibie,jine  from ( select riqi,leibie,jine, row_number() over (partition by leibie order by jine desc) rank from sales) t "
                + "where t.rank<=2").show();

        spark.stop();
    }
}
