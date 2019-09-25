package com.lcy.java.demo.ml;

import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaIndexToStringExample {
    
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession
                .builder()
                .appName("indexToString")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        
        StructType schema = new StructType(new StructField[]{
                new StructField("id", IntegerType, false, Metadata.empty()),
                new StructField("category", StringType, false, Metadata.empty())
        });
    
        Dataset<Row> df = spark.createDataFrame(data, schema);
    
        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
    
        Dataset<Row> indexed = indexer.transform(df);
        indexed.show();
        
        IndexToString converter = new IndexToString()
                .setInputCol("categoryIndex")
                .setOutputCol("originalIndex");
        
        converter.transform(indexed).show();
        
        spark.stop();
        
    }
}
