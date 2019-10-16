package com.lcy.java.spark.ml;

import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaLinearSVCExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaLinearSVCExample")
                .getOrCreate();
        
        // Load training data
        Dataset<Row> training = spark.read().format("libsvm")
                .load("data/mllib/sample_libsvm_data.txt");
        
        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(10)
                .setRegParam(0.1);
        
        // Fit the model
        LinearSVCModel lsvcModel = lsvc.fit(training);
        
        // Print the coefficients and intercept for LinearSVC
        System.out.println("Coefficients: "
                + lsvcModel.coefficients() + " Intercept: " + lsvcModel.intercept());
        
        spark.stop();
    }
}
