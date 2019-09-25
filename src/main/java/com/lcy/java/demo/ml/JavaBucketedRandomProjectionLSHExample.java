package com.lcy.java.demo.ml;

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
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

import static org.apache.spark.sql.functions.col;

public class JavaBucketedRandomProjectionLSHExample {
    
    @Test
    public void test1(){
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaBucketedRandomProjectionLSH")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    
        List<Row> dataA = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(1, Vectors.dense(1.0, -1.0)),
                RowFactory.create(2, Vectors.dense(-1.0, -1.0)),
                RowFactory.create(3, Vectors.dense(-1.0, 1.0))
        );
    
        List<Row> dataB = Arrays.asList(
                RowFactory.create(4, Vectors.dense(1.0, 0.0)),
                RowFactory.create(5, Vectors.dense(-1.0, 0.0)),
                RowFactory.create(6, Vectors.dense(0.0, 1.0)),
                RowFactory.create(7, Vectors.dense(0.0, -1.0))
        );
    
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
        Dataset<Row> dfB = spark.createDataFrame(dataB, schema);
    
        Vector key = Vectors.dense(1.0, 0.0);
    
        BucketedRandomProjectionLSH mh = new BucketedRandomProjectionLSH()
                .setBucketLength(2.0)
                .setNumHashTables(3)
                .setInputCol("features")
                .setOutputCol("hashes");
    
        BucketedRandomProjectionLSHModel model = mh.fit(dfA);
    
        //特征转换
        System.out.println("散列值存储在“散列”列中的散列数据集：");
        model.transform(dfA).show(false);
    
        //计算输入行的位置敏感哈希，然后执行近似
        //相似连接。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxSimilarityJoin（transformedA，transformedB，1.5）`
        System.out.println("在小于1.5的距离处大约连接dfA和dfB：");
        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("EuclideanDistance"))
                .show(false);
    
        //计算输入行的位置敏感哈希，然后执行近似最近
        //邻居搜索。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxNearestNeighbors（transformedA，key，2）`
        System.out.println("A在dfA附近搜索密钥的2个最近邻居：");
        model.approxNearestNeighbors(dfA, key, 2).show(false);
    
        spark.stop();
    }
    
    
    @Test
    public void test2(){
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaBucketedRandomProjectionLSH")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    
        List<Row> dataA = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(1, Vectors.dense(1.0, -1.0)),
                RowFactory.create(2, Vectors.dense(-1.0, -1.0)),
                RowFactory.create(3, Vectors.dense(-1.0, 1.0))
        );
    
        List<Row> dataB = Arrays.asList(
                RowFactory.create(4, Vectors.dense(1.0, 0.0)),
                RowFactory.create(5, Vectors.dense(-1.0, 0.0)),
                RowFactory.create(6, Vectors.dense(0.0, 1.0)),
                RowFactory.create(7, Vectors.dense(0.0, -1.0))
        );
    
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(dataA, schema);
        Dataset<Row> dfB = spark.createDataFrame(dataB, schema);
    
        Vector key = Vectors.dense(1.0, 0.0);
    
        BucketedRandomProjectionLSH mh = new BucketedRandomProjectionLSH()
                .setBucketLength(2.0)
                .setNumHashTables(3)
                .setInputCol("features")
                .setOutputCol("hashes");
    
        BucketedRandomProjectionLSHModel model = mh.fit(dfA);
    
        System.out.println("散列值存储在“散列”列中的散列数据集：");
        model.transform(dfA).show(false);
    
        System.out.println("在小于1.5的距离处大约连接dfA和dfB：");
        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("EuclideanDistance"))
                .show(false);
    
        System.out.println("A在dfA附近搜索密钥的2个最近邻居：");
        model.approxNearestNeighbors(dfA, key, 2).show(false);
    
        spark.stop();
    }
    
}
