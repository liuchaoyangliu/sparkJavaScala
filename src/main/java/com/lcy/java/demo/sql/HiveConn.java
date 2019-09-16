package com.lcy.java.demo.sql;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * window测试不可以，需要打成.jar包放入Linux环境运行
 */

public class HiveConn {

    SparkSession spark = null;

    @Before
    public void before(){
        spark = SparkSession
                .builder()
                .master("local")
                .appName("HiveConnApp")
                .enableHiveSupport()
                .getOrCreate();
    }

    @Test
    public void test1(){
        spark.sql("show databases").show();
        spark.sql("show tables").show();
//        spark.sql("select * from student").show();
    }

    @Test
    public void test2(){

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation) //设置表的放置位置
//                .config("hadoop.home.dir", "/user/hive/warehouse")  //设置表的放置位置
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'file:/home/ubuntu/sparkData/hive.txt' INTO TABLE src");

        spark.sql("SELECT * FROM src").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        spark.sql("SELECT COUNT(*) FROM src").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+

        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

        Dataset<String> stringsDS = sqlDF.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                Encoders.STRING());
        stringsDS.show();
        // +--------------------+
        // |               value|
        // +--------------------+
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // ...

        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        spark.sql("SELECT * FROM records r " +
                " src s ON r.key = s.key").show();
        // +---+------+---+------+
        // |key| value|key| value|
        // +---+------+---+------+
        // |  2| val_2|  2| val_2|
        // |  2| val_2|  2| val_2|
        // |  4| val_4|  4| val_4|
        // ...

    }

    @After
    public void after(){
        spark.stop();
    }

}


/**
 *
238 val_238
86 val_86
311 val_311
27 val_27
165 val_165
409 val_409
255 val_255
278 val_278
98 val_98
484 val_484
265 val_265
193 val_193
401 val_401
150 val_150
273 val_273-
224 val_224
369 val_369
66 val_66
128 val_128
 *
 */