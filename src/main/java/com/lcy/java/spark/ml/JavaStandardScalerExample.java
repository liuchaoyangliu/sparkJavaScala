package com.lcy.java.spark.ml;

import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaStandardScalerExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStandardScaler")
                .getOrCreate();
        
        Dataset<Row> dataFrame = spark
                        .read()
                        .format("libsvm")
                        .load("file:\\D:\\sparkData\\sample_libsvm_data.txt");
        
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);
        
        // 通过安装StandardScaler计算汇总统计信息
        StandardScalerModel scalerModel = scaler.fit(dataFrame);
        
        // 标准化每个特征以具有单位标准偏差。
        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
        spark.stop();
    }
}
