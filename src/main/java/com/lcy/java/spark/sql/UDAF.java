package com.lcy.java.spark.sql;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class UDAF {

    @Test
    public void test(){

        SparkSession spark = SparkSession.builder()
                .appName("study")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        spark.udf().register("myAverage", new MyAverage());
        spark.read().json("file:\\D:\\sparkData\\employees.json").createOrReplaceTempView("employees");
        spark.sql("SELECT myAverage(salary) as average_salary FROM employees").show();

    }

}
