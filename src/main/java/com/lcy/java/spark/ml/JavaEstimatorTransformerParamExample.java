package com.lcy.java.spark.ml;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Java example for Estimator, Transformer, and Param.
 */
public class JavaEstimatorTransformerParamExample {



    @Test
    public void test1(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaEstimatorTransformerParamExample")
                .getOrCreate();

        // 准备培训数据。
        List<Row> dataTraining = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> training = spark.createDataFrame(dataTraining, schema);

        // 创建LogisticRegression实例。这个实例是一个Estimator。
        LogisticRegression lr = new LogisticRegression();
        // 打印出参数，文档和任何默认值。
        System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");

        // 我们可以使用setter方法设置参数。
        lr.setMaxIter(10).setRegParam(0.01);

        // 学习LogisticRegression模型。这使用存储在lr中的参数。
        LogisticRegressionModel model1 = lr.fit(training);
        //由于model1是Model（即由Estimator生成的Transformer），
        // 我们可以查看fit（）期间使用的参数。
        // 这将打印参数（name：value）对，其中names是此
        // LogisticRegression实例的唯一ID。
        System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

        // 我们也可以使用ParamMap指定参数。
        ParamMap paramMap = new ParamMap()
                .put(lr.maxIter().w(20))  // Specify 1 Param.
                .put(lr.maxIter(), 30)  // 这会覆盖原始的maxIter。
                .put(lr.regParam().w(0.1), lr.threshold().w(0.55));  // 指定多个参数。

        // 也可以组合ParamMaps。
        ParamMap paramMap2 = new ParamMap()
                .put(lr.probabilityCol().w("myProbability"));  // 更改输出列名称
        ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

        //现在使用paramMapCombined参数学习一个新模型。
        // paramMapCombined通过lr.set *方法覆盖之前设置的所有参数。
        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

        // 准备测试文件。
        List<Row> dataTest = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
                RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))
        );
        Dataset<Row> test = spark.createDataFrame(dataTest, schema);

        //使用Transformer.transform（）方法对测试文档进行预测。
        // LogisticRegression.transform仅使用“功能”列。
        // 注意，model2.transform（）输出'myProbability'列而
        // 不是通常的''probability'列，因为我们先前重命名了lr.probabilityCol参数。
        Dataset<Row> results = model2.transform(test);
        Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
        for (Row r: rows.collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        spark.stop();

    }


    @Test
    public void test2(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaEstimatorTransformerParamExample")
                .getOrCreate();

        List<Row> dataTraining = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> training = spark.createDataFrame(dataTraining, schema);

        LogisticRegression lr = new LogisticRegression();
        System.out.println("LogisticRegression parameters(逻辑回归参数):\n" + lr.explainParams() + "\n");

        lr.setMaxIter(10).setRegParam(0.01);

        LogisticRegressionModel model1 = lr.fit(training);

        System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

        ParamMap paramMap = new ParamMap()
                .put(lr.maxIter().w(20))
                .put(lr.maxIter(), 30)
                .put(lr.regParam().w(0.1), lr.threshold().w(0.55));

        ParamMap paramMap2 = new ParamMap()
                .put(lr.probabilityCol().w("myProbability"));
        ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

        List<Row> dataTest = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
                RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))
        );
        Dataset<Row> test = spark.createDataFrame(dataTest, schema);

        Dataset<Row> results = model2.transform(test);
        Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
        for (Row r: rows.collectAsList()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }

        spark.stop();

    }

}
