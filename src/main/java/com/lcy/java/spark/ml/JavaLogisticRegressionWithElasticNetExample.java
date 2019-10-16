package com.lcy.java.spark.ml;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaLogisticRegressionWithElasticNetExample {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaLogisticRegressionWithElasticNet")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        
        Dataset<Row> training = spark.read().format("libsvm")
                .load("file:\\D:\\sparkData\\sample_libsvm_data.txt");
        
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        LogisticRegressionModel lrModel = lr.fit(training);

        // 打印系数并截取逻辑回归
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " \nIntercept: " + lrModel.intercept());

        // 我们还可以使用多项式族进行二进制分类
        LogisticRegression mlr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFamily("multinomial");

        LogisticRegressionModel mlrModel = mlr.fit(training);

        // 打印系数和截距以使用多项式族进行逻辑回归
        System.out.println("Multinomial coefficients: " + lrModel.coefficientMatrix()
                + "\nMultinomial intercepts: " + mlrModel.interceptVector());

        spark.stop();
    }
}
