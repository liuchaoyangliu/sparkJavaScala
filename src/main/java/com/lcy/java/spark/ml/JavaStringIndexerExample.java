package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaStringIndexerExample {
    
    public static void main(String[] args) {
    
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStringIndexer")
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
    
        List<Row> data2 = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c"),
                RowFactory.create(6, "e")
        );
        
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("category", StringType, false)
        });
    
        Dataset<Row> df = spark.createDataFrame(data, schema);
        Dataset<Row> df2 = spark.createDataFrame(data2, schema);
        
        StringIndexer indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex");
        
        indexer.fit(df).transform(df).show();
        
        try{
            indexer.fit(df).transform(df2).show();
        }catch (Exception e){
            e.printStackTrace();
        }
        
        indexer.fit(df).setHandleInvalid("skip").transform(df2).show();
        
        indexer.fit(df).setHandleInvalid("keep").transform(df2).show();
        
        spark.stop();
    }
}
