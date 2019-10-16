package com.lcy.java.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;

public class CreateDataFrame {

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
    public void test1(){

        Dataset<Row> df = spark.read().json("file:/D:/sparkData/people.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

    }

    @Test
    public void test2(){

        Dataset<Row> df = spark.read().json("file:/D:/sparkData/people.json");

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
    }

    /**
     * 3
     * Global Temporary View
     *
     * Spark SQL中的临时视图是会话范围的，如果创建它的会话终止，它将消失。如果您希望拥有一
     * 个在所有会话之间共享的临时视图并保持活动状态，直到Spark应用程序终止，您可以创建一个
     * 全局临时视图。全局临时视图与系统保留的数据库绑定global_temp，我们必须使用限定名
     * 称来引用它，例如SELECT * FROM global_temp.view1。
     */
    @Test
    public void test3() throws Exception{

        Dataset<Row> df = spark.read().json("file:/D:/sparkData/people.json");

        df.createGlobalTempView("people");

        spark.sql("SELECT * FROM global_temp.people").show();

        spark.newSession().sql("SELECT * FROM global_temp.people").show();

    }

    @Test
    public void test4(){

        /**
         * 使用反射推断模式
         */
        JavaRDD<Person> javaRDD = spark.read()
                .textFile("file:/D:/sparkData/people.txt")
                .toJavaRDD()
                .map(line -> {
                    String[] split = line.split(",");
                    return new Person(split[0], Integer.parseInt(split[1]));
                });

        Dataset<Row> dataFrame = spark.createDataFrame(javaRDD, Person.class);
        dataFrame.createOrReplaceTempView("people");

        spark.sql("SELECT * FROM people").show();

        Dataset<Row> rowDataset = spark.sql("select name from people");
        Encoder<String> stringEncoder = Encoders.STRING();
        rowDataset.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0), stringEncoder)
                .show();
    }

    @Test
    public void test5(){

        /**
         * 以编程方式指定架构
         *
         * 1,Row从原始RDD 创建s的RDD;
         * 2,创建由StructType匹配Row步骤1中创建的RDD中的s 结构 表示的模式。
         * 3,Row通过createDataFrame提供的方法将模式应用于s 的RDD SparkSession。
         */

        JavaRDD<String> peopleRdd = spark.sparkContext()
                .textFile("file:/D:/sparkData/people.txt", 1)
                .toJavaRDD();

        String schemaString = "name,age";
        List<StructField> fields = new ArrayList<>();
        for(String fieldName : schemaString.split(",")){
            StructField structField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(structField);
        }
        StructType structType = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = peopleRdd.map(line -> {
            String[] strings = line.split(",");
            return RowFactory.create(strings[0], strings[1].trim());
        });

        Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, structType);

        dataFrame.createOrReplaceTempView("people");
        spark.sql("select * from people").show();

//        Dataset<Row> results = spark.sql("SELECT * FROM people");
//        Dataset<String> namesDS = results.map(
//                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
//                Encoders.STRING());
//        namesDS.show();

        spark.sql("select name,age from people")
                .toJavaRDD()
                .map(row ->
                        new Tuple2(row.<String>getAs("name"), row.<String>getAs("age")))
                .foreach(e -> System.out.println(e._1 + " " + e._2));


        /**
         *将数据写入数据库
         */

//        String url = "jdbc:mysql://localhost:3306/spark?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true";
//        Properties properties = new Properties();
//        properties.setProperty("user","root");
//        properties.setProperty("password","root");
//
//        dataFrame.write().jdbc(url, "people", properties);
//        /**
//         * 保存到本地
//         */
//        dataFrame.toJavaRDD().saveAsObjectFile("file:/D:/sparkData/file");
//        dataFrame.write().json("file:/D:/sparkData/json");

    }

    @Test
    public void test6(){

    }




    @Test
    public void runJdbcDatasetExample(){

        String url = "jdbc:mysql://localhost:3306";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        Dataset<Row> jdbcDF = spark.read()
                .jdbc(url, "spark.log", connectionProperties);

        jdbcDF.createOrReplaceTempView("log");

        spark.sql("select * from log where id <= 70").show();

        jdbcDF.write()
                .jdbc(url, "spark.log2", connectionProperties);

    }

    @Test
    public void loadSave(){
        Dataset<Row> df = spark.read().load("file:/D:/sparkData/people.parquet");
        df.printSchema();
        df.show();
        df.select("Id", "CreatedTime").write().save("file:/D:/sparkData/people");
    }

    @Test
    public void specifyFormatLoadSave(){
        Dataset<Row> df = spark.read().format("json").load("file:/D:/sparkData/people.json");
        df.select("name").write().format("parquet").save("file:/D:/sparkData/parquet");
    }


    @After
    public void after(){
        spark.stop();
    }

}







