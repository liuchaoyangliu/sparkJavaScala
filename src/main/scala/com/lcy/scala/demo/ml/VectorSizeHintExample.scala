package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * 有时明确指定VectorType列的向量大小可能很有用。例如，VectorAssembler使用其输入列
 * 中的大小信息来为其输出列生成大小信息和元数据。尽管在某些情况下，可以通过检查列的内容来获取此信息，
 * 但是在流数据帧中，只有在流启动后，内容才可用。 VectorSizeHint允许用户显式指定列的向量大小，
 * 以便VectorAssembler或可能需要知道向量大小的其他转换器可以将该列用作输入。
 *
 * 要使用VectorSizeHint，用户必须设置inputCol和size参数。将此转换器应用于数据框将生成一个新的数据框，
 * 其中包含用于inputCol的更新元数据，以指定矢量大小。生成的数据帧上的下游操作可以使用Meatadata获得此大小。
 *
 * VectorSizeHint也可以采用可选的handleInvalid参数，当vector列包含null或错误大小的vector时，
 * 该参数控制其行为。默认情况下，handleInvalid设置为“错误”，表示应引发异常。此参数也可以设置为“跳过”，
 * 指示应从结果数据框中过滤出包含无效值的行，或“乐观”，指示不应检查该列的无效值，而应保留所有行。
 * 请注意，使用“乐观”可能会导致结果数据帧处于不一致状态，即：VectorVectorHint列所应用的元数据
 * 与该列的内容不匹配。用户应注意避免这种不一致的状态。
 */

object VectorSizeHintExample {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("VectorSizeHint")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataset = spark.createDataFrame(
            Seq(
                (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
                (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
        ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

        val sizeHint = new VectorSizeHint()
                .setInputCol("userFeatures")
                .setHandleInvalid("skip")
                .setSize(3)

        val datasetWithSize = sizeHint.transform(dataset)
        println("过滤掉'userFeatures'不正确大小的行")
        datasetWithSize.show(false)
        //        +---+----+------+--------------+-------+
        //        |id |hour|mobile|userFeatures  |clicked|
        //        +---+----+------+--------------+-------+
        //        |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
        //        +---+----+------+--------------+-------+


        val assembler = new VectorAssembler()
                .setInputCols(Array("hour", "mobile", "userFeatures"))
                .setOutputCol("features")

        // 这个数据帧可以像以前一样由下游变压器使用
        val output = assembler.transform(datasetWithSize)
        println("汇总列'小时'，'移动'，'userFeatures'到矢量列'功能'")
        //        output.select("features", "clicked").show(false)
        output.show(false)
        //        +---+----+------+--------------+-------+-----------------------+
        //        |id |hour|mobile|userFeatures  |clicked|features               |
        //        +---+----+------+--------------+-------+-----------------------+
        //        |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |[18.0,1.0,0.0,10.0,0.5]|
        //        +---+----+------+--------------+-------+-----------------------+

        spark.stop()
    }
}
