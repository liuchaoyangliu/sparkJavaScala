package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * 局部敏感哈希(Locality Sensitive Hashing，LSH)
 * LSH最根本的作用，就是能高效处理海量高维数据的最近邻问题
 *
 * 局部敏感哈希的基本思想类似于一种空间域转换思想，LSH算法基于一个假设，如果两个文本在原有的
 * 数据空间是相似的，那么分别经过哈希函数转换以后的它们也具有很高的相似度；相反，如果它们本身
 * 是不相似的，那么经过转换后它们应仍不具有相似性。
 */

//欧氏距离的桶式随机投影
object BucketedRandomProjectionLSHExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("BucketedRandomProjectionLSH")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dfA = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 1.0)),
            (1, Vectors.dense(1.0, -1.0)),
            (2, Vectors.dense(-1.0, -1.0)),
            (3, Vectors.dense(-1.0, 1.0))
        )).toDF("id", "features")

        val dfB = spark.createDataFrame(Seq(
            (4, Vectors.dense(1.0, 0.0)),
            (5, Vectors.dense(-1.0, 0.0)),
            (6, Vectors.dense(0.0, 1.0)),
            (7, Vectors.dense(0.0, -1.0))
        )).toDF("id", "features")

        val key = Vectors.dense(1.0, 0.0)

        val brp = new BucketedRandomProjectionLSH()

                /**
                 * 每个哈希存储桶的长度，较大的存储桶可降低误报率。的数量
                 * buckets将是`（输入向量的最大L2范数）/ bucketLength`。
                 *
                 * 如果将输入向量标准化，则pow（numRecords，-1 / inputDim）的1-10倍将是
                 * 合理的价值
                 */
                .setBucketLength(2.0)

                /**
                 * LSH OR放大中使用的哈希表数量的参数。
                 *
                 * LSH OR放大可用于减少假阴性率。更高的价值
                 * 参数会导致误报率降低，但会增加计算复杂性。
                 *
                 * @组参数
                 */
                .setNumHashTables(3)
                .setInputCol("features")
                .setOutputCol("hashes")

        val model = brp.fit(dfA)

        //特征转换
        println("散列值存储在“散列”列中的散列数据集：")
        model.transform(dfA).show(false)
        //  +---+-----------+-----------------------+
        //  |id |features   |hashes                 |
        //  +---+-----------+-----------------------+
        //  |0  |[1.0,1.0]  |[[0.0], [0.0], [-1.0]] |
        //  |1  |[1.0,-1.0] |[[-1.0], [-1.0], [0.0]]|
        //  |2  |[-1.0,-1.0]|[[-1.0], [-1.0], [0.0]]|
        //  |3  |[-1.0,1.0] |[[0.0], [0.0], [-1.0]] |
        //  +---+-----------+-----------------------+

        //计算输入行的位置敏感哈希，然后执行近似
        //相似连接。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxSimilarityJoin（transformedA，transformedB，1.5）`
        println("在小于1.5的欧氏距离上近似连接dfA和dfB：")

        /**
         * 连接两个数据集以大致找到距离小于的所有行对
         * 门槛。如果缺少[[outputCol]]，则该方法将转换数据；否则，将转换数据。如果
         * [[[outputCol]]存在，它将使用[[outputCol]]。这样可以缓存已转换的
         * 必要时提供数据。
         *
         * param datasetA要加入的数据集之一  。
         * param datasetB另一个要加入的数据集 。
         * param threshold行对距离的阈值   。
         * param distCol输出列，用于存储每对行之间的距离。
         * return包含行对的联接数据集。原始行在列中
         * “ datasetA”和“ datasetB”，并添加了列“ distCol”以显示距离
         * 每对之间。
         */
        //        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
        //                .select(
        //                    col("datasetA.id").alias("idA"),
        //                    col("datasetB.id").alias("idB"),
        //                    col("EuclideanDistance")
        //                ).show(false)
        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
                .show(false)
        //  +-----------------------------------------+----------------------------------------+-----------------+
        //  |datasetA                                 |datasetB                                |EuclideanDistance|
        //  +-----------------------------------------+----------------------------------------+-----------------+
        //  |[1, [1.0,-1.0], [[-1.0], [-1.0], [0.0]]] |[4, [1.0,0.0], [[0.0], [-1.0], [-1.0]]] |1.0              |
        //  |[0, [1.0,1.0], [[0.0], [0.0], [-1.0]]]   |[6, [0.0,1.0], [[0.0], [0.0], [-1.0]]]  |1.0              |
        //  |[1, [1.0,-1.0], [[-1.0], [-1.0], [0.0]]] |[7, [0.0,-1.0], [[-1.0], [-1.0], [0.0]]]|1.0              |
        //  |[3, [-1.0,1.0], [[0.0], [0.0], [-1.0]]]  |[5, [-1.0,0.0], [[-1.0], [0.0], [0.0]]] |1.0              |
        //  |[0, [1.0,1.0], [[0.0], [0.0], [-1.0]]]   |[4, [1.0,0.0], [[0.0], [-1.0], [-1.0]]] |1.0              |
        //  |[3, [-1.0,1.0], [[0.0], [0.0], [-1.0]]]  |[6, [0.0,1.0], [[0.0], [0.0], [-1.0]]]  |1.0              |
        //  |[2, [-1.0,-1.0], [[-1.0], [-1.0], [0.0]]]|[7, [0.0,-1.0], [[-1.0], [-1.0], [0.0]]]|1.0              |
        //  |[2, [-1.0,-1.0], [[-1.0], [-1.0], [0.0]]]|[5, [-1.0,0.0], [[-1.0], [0.0], [0.0]]] |1.0              |
        //  +-----------------------------------------+----------------------------------------+-----------------+


        //计算输入行的位置敏感哈希，然后执行近似最近
        //邻居搜索。
        //我们可以通过传入已转换的数据集来避免计算哈希，例如
        //`model.approxNearestNeighbors（transformedA，key，2）`
        println("大约在dfA中搜索密钥的2个最近邻居：")

        /**
         * 重载了aboutNearestNeighbors的方法。使用“ distCol”作为默认distCol。
         */
        model.approxNearestNeighbors(dfA, key, 2).show(false)
        //  +---+----------+-----------------------+-------+
        //  |id |features  |hashes                 |distCol|
        //  +---+----------+-----------------------+-------+
        //  |0  |[1.0,1.0] |[[0.0], [0.0], [-1.0]] |1.0    |
        //  |1  |[1.0,-1.0]|[[-1.0], [-1.0], [0.0]]|1.0    |
        //  +---+----------+-----------------------+-------+

        spark.stop()
    }


    //    def main(args: Array[String]): Unit = {
    //
    //        val spark = SparkSession
    //                .builder()
    //                .appName("bucketedRandomProjectionLSH")
    //                .master("local")
    //                .getOrCreate()
    //        spark.sparkContext.setLogLevel("ERROR")
    //
    //        val dfA = spark.createDataFrame(Seq(
    //            (0, Vectors.dense(1.0, 1.0)),
    //            (1, Vectors.dense(1.0, -1.0)),
    //            (2, Vectors.dense(-1.0, -1.0)),
    //            (3, Vectors.dense(-1.0, 1.0))
    //        )).toDF("id", "features")
    //
    //        val dfB = spark.createDataFrame(Seq(
    //            (0, Vectors.dense(1.0, 1.0)),
    //            (1, Vectors.dense(1.0, -1.0)),
    //            (2, Vectors.dense(-1.0, -1.0)),
    //            (3, Vectors.dense(-1.0, 1.0))
    //        )).toDF("id", "features")
    //
    //        val key = Vectors.dense(1.0, 0.0)
    //
    //        val brp = new BucketedRandomProjectionLSH()
    //                .setBucketLength(2.0)
    //                .setNumHashTables(3)
    //                .setInputCol("features")
    //                .setOutputCol("hashes")
    //
    //        val model = brp.fit(dfA)
    //
    //        model.transform(dfA).show(false)
    //        //  +---+-----------+-----------------------+
    //        //  |id |features   |hashes                 |
    //        //  +---+-----------+-----------------------+
    //        //  |0  |[1.0,1.0]  |[[0.0], [0.0], [-1.0]] |
    //        //  |1  |[1.0,-1.0] |[[-1.0], [-1.0], [0.0]]|
    //        //  |2  |[-1.0,-1.0]|[[-1.0], [-1.0], [0.0]]|
    //        //  |3  |[-1.0,1.0] |[[0.0], [0.0], [-1.0]] |
    //        //  +---+-----------+-----------------------+
    //
    //
    //        //        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
    //        //                .select(
    //        //                    col("datasetA.id").alias("idA"),
    //        //                    col("datasetB.id").alias("idB"),
    //        //                    col("EuclideanDistance")
    //        //                ).show(false)
    //        model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
    //                .show(false)
    //        //  +-----------------------------------------+-----------------------------------------+-----------------+
    //        //  |datasetA                                 |datasetB                                 |EuclideanDistance|
    //        //  +-----------------------------------------+-----------------------------------------+-----------------+
    //        //  |[0, [1.0,1.0], [[0.0], [0.0], [-1.0]]]   |[0, [1.0,1.0], [[0.0], [0.0], [-1.0]]]   |0.0              |
    //        //  |[3, [-1.0,1.0], [[0.0], [0.0], [-1.0]]]  |[3, [-1.0,1.0], [[0.0], [0.0], [-1.0]]]  |0.0              |
    //        //  |[2, [-1.0,-1.0], [[-1.0], [-1.0], [0.0]]]|[2, [-1.0,-1.0], [[-1.0], [-1.0], [0.0]]]|0.0              |
    //        //  |[1, [1.0,-1.0], [[-1.0], [-1.0], [0.0]]] |[1, [1.0,-1.0], [[-1.0], [-1.0], [0.0]]] |0.0              |
    //        //  +-----------------------------------------+-----------------------------------------+-----------------+
    //
    //        model.approxNearestNeighbors(dfA, key, 2).show(false)
    //        //  +---+----------+-----------------------+-------+
    //        //  |id |features  |hashes                 |distCol|
    //        //  +---+----------+-----------------------+-------+
    //        //  |0  |[1.0,1.0] |[[0.0], [0.0], [-1.0]] |1.0    |
    //        //  |1  |[1.0,-1.0]|[[-1.0], [-1.0], [0.0]]|1.0    |
    //        //  +---+----------+-----------------------+-------+
    //
    //        spark.stop()
    //    }


}
