package com.lcy.java.demo.ml;

import org.apache.spark.ml.regression.IsotonicRegression;
import org.apache.spark.ml.regression.IsotonicRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaIsotonicRegressionExample {
    
    public static void main(String[] args) {
        // Create a SparkSession.
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaIsotonicRegressionExample")
                .getOrCreate();
        
        // Loads data.
        Dataset<Row> dataset = spark.read().format("libsvm")
                .load("data/mllib/sample_isotonic_regression_libsvm_data.txt");
        
        // Trains an isotonic regression model.
        IsotonicRegression ir = new IsotonicRegression();
        IsotonicRegressionModel model = ir.fit(dataset);
        
        System.out.println("Boundaries in increasing order: " + model.boundaries() + "\n");
        System.out.println("Predictions associated with the boundaries: " + model.predictions() + "\n");
        
        // Makes predictions.
        model.transform(dataset).show();
        
        spark.stop();
    }
}
