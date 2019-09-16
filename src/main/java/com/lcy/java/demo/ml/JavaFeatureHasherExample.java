package com.lcy.java.demo.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.FeatureHasher;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaFeatureHasherExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaFeatureHasherExample")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> data = Arrays.asList(
                RowFactory.create(2.2, true, "1", "foo"),
                RowFactory.create(3.3, false, "2", "bar"),
                RowFactory.create(4.4, false, "3", "baz"),
                RowFactory.create(5.5, false, "4", "foo")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("real", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("bool", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("stringNum", DataTypes.StringType, false, Metadata.empty()),
                new StructField("string", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dataset = spark.createDataFrame(data, schema);

        FeatureHasher hasher = new FeatureHasher()
                .setInputCols(new String[]{"real", "bool", "stringNum", "string"})
                .setOutputCol("features");

        Dataset<Row> featurized = hasher.transform(dataset);

        featurized.show(false);

        spark.stop();

    }

}
