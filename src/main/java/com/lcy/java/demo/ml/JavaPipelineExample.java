package com.lcy.java.demo.ml;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Java example for simple text document 'Pipeline'.
 */

public class JavaPipelineExample {
    
    @Test
    public void test1(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaPipelineExample")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // 准备标有的培训文件。
        Dataset<Row> training = spark.createDataFrame(Arrays.asList(
                new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
                new JavaLabeledDocument(1L, "b d", 0.0),
                new JavaLabeledDocument(2L, "spark f g h", 1.0),
                new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0)
        ), JavaLabeledDocument.class);

        // 配置ML管道，包括三个阶段：tokenizer，hashingTF和lr。
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

        // 使管道适合培训文件。
        PipelineModel model = pipeline.fit(training);

        // 准备未标记的测试文档。
        Dataset<Row> test = spark.createDataFrame(Arrays.asList(
                new JavaDocument(4L, "spark i j k"),
                new JavaDocument(5L, "l m n"),
                new JavaDocument(6L, "spark hadoop spark"),
                new JavaDocument(7L, "apache hadoop")
        ), JavaDocument.class);

        // 对测试文档进行预测。
        Dataset<Row> predictions = model.transform(test);
        for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        spark.stop();
    }


    @Test
    public void test2(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaPipelineExample")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // 准备标有的培训文件。
        Dataset<Row> training = spark.createDataFrame(Arrays.asList(
                new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
                new JavaLabeledDocument(1L, "b d", 0.0),
                new JavaLabeledDocument(2L, "spark f g h", 1.0),
                new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0)
        ), JavaLabeledDocument.class);

        // 配置ML管道，包括三个阶段：tokenizer，hashingTF和lr。
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

        // 使管道适合培训文件。
        PipelineModel model = pipeline.fit(training);

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });

        // 准备未标记的测试文档。
        Dataset<Row> test = spark.createDataFrame(Arrays.asList(
                RowFactory.create(4L, "spark i j k"),
                RowFactory.create(5L, "l m n"),
                RowFactory.create(6L, "spark hadoop spark"),
                RowFactory.create(7L, "apache hadoop")
        ), schema);

        // 对测试文档进行预测。
        Dataset<Row> predictions = model.transform(test);
        for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        spark.stop();
    }
    
}
