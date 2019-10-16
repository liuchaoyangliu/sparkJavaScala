package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class JavaTokenizerExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaTokenizerExample")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");

        spark.udf().register(
                "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

        Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
        tokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", (Seq<Column>) col("words")))
                .show(false);

        Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", (Seq<Column>) col("words")))
                .show(false);

        spark.stop();

    }

}
