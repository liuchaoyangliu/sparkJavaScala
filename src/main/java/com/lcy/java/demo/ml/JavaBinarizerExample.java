package com.lcy.java.demo.ml;

import org.apache.spark.ml.feature.Binarizer;
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

public class JavaBinarizerExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaBinarizer")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.1),
                RowFactory.create(1, 0.8),
                RowFactory.create(2, 0.2)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> continuousDataFrame = spark.createDataFrame(data, schema);

        Binarizer binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5);

        Dataset<Row> binarizedDataFrame = binarizer.transform(continuousDataFrame);

        System.out.println("具有阈值的二进制化器输出 = " + binarizer.getThreshold());
        binarizedDataFrame.show();

        spark.stop();

    }

}
