package com.lcy.java.demo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class JdbcConn {

    SparkSession spark = null;

    @Before
    public void before(){
        spark = SparkSession.builder().appName("SparkStudy").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @Test
    public void test1(){
        String url = "jdbc:mysql://localhost:3306/spark?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true";
        String table ="people";

        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "root");

        Dataset<Row> rowDataset =
                spark.read()
                .jdbc(url, table, properties);

        rowDataset.show();

//        // 指定读取模式的自定义数据类型
//        // 此方法未理解
//        properties.put("customSchema", "id DECIMAL(32, 0), content STRING");
//        Dataset<Row> rowDataset2 = spark.read()
//                .jdbc(url, table, properties);
//
//        rowDataset2.show();
    }

    @After
    public void after(){
        spark.stop();
    }


}
