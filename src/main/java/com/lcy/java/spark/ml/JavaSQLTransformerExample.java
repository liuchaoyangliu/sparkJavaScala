package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class JavaSQLTransformerExample {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSQLTransformer")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 1.0, 3.0),
                RowFactory.create(2, 2.0, 5.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("v1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("v2", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        SQLTransformer sqlTrans = new SQLTransformer()
                .setStatement(
                "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");
        
        sqlTrans.transform(df).show();
        
        spark.stop();
    }
}
