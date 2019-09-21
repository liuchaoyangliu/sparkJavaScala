package com.lcy.java.demo.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class JavaMinHashLSHExample {
    
    public static void main(String[] args) {
        
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaMinHashLSH")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        
        List<Row> dataA = Arrays.asList(
                RowFactory.create(0, Vectors.sparse(6, new int[]{0, 1, 2}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 4}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(2, Vectors.sparse(6, new int[]{0, 2, 4}, new double[]{1.0, 1.0, 1.0}))
        );
        
        List<Row> dataB = Arrays.asList(
                RowFactory.create(0, Vectors.sparse(6, new int[]{1, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(1, Vectors.sparse(6, new int[]{2, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(2, Vectors.sparse(6, new int[]{1, 2, 4}, new double[]{1.0, 1.0, 1.0}))
        );
        
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
        Dataset<Row> dfB = spark.createDataFrame(dataB, schema);
        
        int[] indices = {1, 3};
        double[] values = {1.0, 1.0};
        Vector key = Vectors.sparse(6, indices, values);
        
        MinHashLSH mh = new MinHashLSH()
                .setNumHashTables(5)
                .setInputCol("features")
                .setOutputCol("hashes");
        
        MinHashLSHModel model = mh.fit(dfA);
        
        //特征转换
        System.out.println("散列值存储在“散列”列中的散列数据集：");
        model.transform(dfA).show(false);
        
       //计算输入行的位置敏感哈希，然后执行近似
        //相似连接。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxSimilarityJoin（transformedA，transformedB，0.6）`
        System.out.println("在Jaccard距离小于0.6的情况下大约加入dfA和dfB：");
        model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
                .select(
                        col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("JaccardDistance")
                ).show(false);
        
        //计算输入行的位置敏感哈希，然后执行近似最近
        //邻居搜索。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxNearestNeighbors（transformedA，key，2）`
        //如果没有足够的近似邻居候选者，则可能返回少于2行
        //找到。
        System.out.println("大约在dfA中搜索密钥的2个最近邻居：");
        model.approxNearestNeighbors(dfA, key, 2).show(false);
        
        spark.stop();
        
    }
    
}
