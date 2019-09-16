package com.lcy.java.demo.ml;

import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

/**
 * 假设检验
 */

public class JavaChiSquareTestExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaChiSquareTestExample")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Vectors.dense(0.5, 10.0)),
                RowFactory.create(0.0, Vectors.dense(1.5, 20.0)),
                RowFactory.create(1.0, Vectors.dense(1.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 40.0)),
                RowFactory.create(1.0, Vectors.dense(3.5, 40.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);
        Row r = ChiSquareTest.test(df, "features", "label").head();
        System.out.println("pValues(p值): " + r.get(0).toString());
        System.out.println("degreesOfFreedom(自由度): " + r.getList(1).toString());
        System.out.println("statistics(统计值): " + r.get(2).toString());

        spark.stop();

    }

}
