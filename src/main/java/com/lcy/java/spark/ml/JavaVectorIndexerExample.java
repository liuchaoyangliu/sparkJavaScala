package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class JavaVectorIndexerExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaVectorIndexer")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> data = spark
                .read()
                .format("libsvm")
                .load("file:\\D:\\sparkData\\sample_libsvm_data.txt");

        VectorIndexer indexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexed")
                .setMaxCategories(10);
        VectorIndexerModel indexerModel = indexer.fit(data);

        Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
        System.out.print("Chose " + categoryMaps.size() + "\n categorical features:");

        for (Integer feature : categoryMaps.keySet()) {
            System.out.print(" " + feature);
        }
        System.out.println("\n\n\n\n");

        // 使用转换为索引的分类值创建“索引”的新列
        Dataset<Row> indexedData = indexerModel.transform(data);
        indexedData.show(false);

        spark.stop();
    }
}
