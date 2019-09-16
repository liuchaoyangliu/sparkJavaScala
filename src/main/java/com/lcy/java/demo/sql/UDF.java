package com.lcy.java.demo.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * UDF(User-defined functions, UDFs),即用户自定义函数，在Spark Sql的开发中十分常用，
 * UDF对表中的每一行进行函数处理，返回新的值，有些类似与RDD编程中的Map()算子，实际开发中几乎
 * 每个Spark程序都会使用的。
 */

public class UDF {

    @Test
    public void UDf(){
        /**  spark1.6版本 */
        SparkConf conf = new SparkConf().setAppName("study").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> strings = Arrays.asList("Leo", "Marry", "Jack", "Tom");
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField( "name", DataTypes.StringType, true ));
        StructType structType = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = jsc.parallelize(strings, 2)
                .map(line -> RowFactory.create(line));

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowJavaRDD, structType);

        dataFrame.createOrReplaceTempView("names");
        sqlContext.udf().register("strLen", (String str) -> str.length(), DataTypes.IntegerType);

        sqlContext.sql("select name,strLen(name) as length from names").show();

    }

    @Test
    public void UDF2(){
        /**  spark2.0之后版本  */
        SparkSession spark = SparkSession.builder()
                .appName("study")
                .master("local")
                .getOrCreate();

        JavaRDD<Row> rowJavaRDD = spark.sparkContext()
                .textFile("file:/D:/sparkData/demo.txt", 1)
                .toJavaRDD()
                .map(line -> RowFactory.create(line));

        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(fields);

        spark.sqlContext().createDataFrame(rowJavaRDD, structType).createOrReplaceTempView("name");
        spark.sqlContext().udf().register("nameLen", (String str) -> str.length(), DataTypes.IntegerType);
        spark.sqlContext().sql("select name,nameLen(name) as length from name").show();

    }

}
