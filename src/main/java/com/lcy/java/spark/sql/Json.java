package com.lcy.java.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Json {

    SparkSession spark = null;

    @Before
    public void before(){
        spark = SparkSession
                .builder()
                .appName("SparkStudy")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    @Test
    public void test2(){
        Dataset<Row> people = spark.read().json("file:/D:/sparkData/people.json");

        people.printSchema();
        people.createOrReplaceTempView("people");

        Dataset<Row> namesDF = spark.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 19");
        namesDF.show();

        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        anotherPeople.show();

    }

    @After
    public void after(){
        spark.stop();
    }


}
