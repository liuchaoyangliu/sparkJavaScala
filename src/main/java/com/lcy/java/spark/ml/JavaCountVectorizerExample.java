package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

public class JavaCountVectorizerExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaCountVectorizerExample")
                .getOrCreate();

        // 输入数据：每行是一个句子或文档中的单词。
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("a", "b", "c")),
                RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 适合语料库中的CountVectorizerModel
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setVocabSize(3)
                .setMinDF(2)
                .fit(df);

        // 或者，使用先验词汇表定义CountVectorizerModel
        CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
                .setInputCol("text")
                .setOutputCol("feature");

        cvModel.transform(df).show(false);

        spark.stop();

    }

}
