package com.lcy.scala.demo.ml

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

//import java.util.Arrays
//
//import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
//import org.apache.spark.ml.feature.VectorSlicer
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.StructType

/**
 *
 * VectorSlicer是一个变换器，它采用一个特征向量并输出一个新的特征向量与原始特征的子数组。
 * 它对于从向量列中提取要素非常有用。
 * VectorSlicer接受具有指定索引的向量列，然后输出一个新的向量列，其值通过这些索引选择。有两种类型的指数，
 * 1,表示向量索引的整数索引setIndices()。
 * 2,字符串索引，表示向量中要素的名称，setNames()。 这要求向量列具有一个，AttributeGroup因为实现
 * 匹配在一个名称字段上Attribute。
 *
 * 整数和字符串的规范都是可以接受的。而且，您可以同时使用整数索引和字符串名称。必须至少选择一项功能。
 * 不允许重复的功能，因此所选索引和名称之间不能有重叠。请注意，如果选择了要素名称，则在遇到空输入属性时将引发异常。
 * 输出向量将首先按所选索引（按给定顺序）对要素进行排序，然后按所选名称（按给定顺序）对要素进行排序。
 *
 */


object VectorSlicerExample {

    //    def main(args: Array[String]): Unit = {
    //
    //        val spark = SparkSession
    //                .builder
    //                .appName("VectorSlicer")
    //                .master("local")
    //                .getOrCreate()
    //        spark.sparkContext.setLogLevel("ERROR")
    //
    //        val data = Arrays.asList(
    //            Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
    //            Row(Vectors.dense(-2.0, 2.3, 0.0))
    //        )
    //
    //        val defaultAttr = NumericAttribute.defaultAttr
    //        val attrs = Array("f1", "f2", "f3")
    //                .map(defaultAttr.withName)
    //        val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
    //
    //        val dataset = spark
    //                .createDataFrame(data, StructType(Array(attrGroup.toStructField())))
    //
    //        val slicer = new VectorSlicer()
    //                .setInputCol("userFeatures")
    //                .setOutputCol("features")
    //
    //        slicer.setIndices(Array(1))
    //                .setNames(Array("f3"))
    //        // or slicer.setIndices(Array(1, 2)),
    //        // or slicer.setNames(Array("f2", "f3"))
    //
    //        val output = slicer.transform(dataset)
    //        output.show(false)
    //
    //        spark.stop()
    //
    //    }


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("VectorSlicer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        //构造特征数组
        val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0)))

        //为特征数组设置属性名（字段名），分别为f1 f2 f3
        val defaultAttr = NumericAttribute.defaultAttr
        val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
        val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

        //构造DataFrame
        val dataRDD = spark.sparkContext.parallelize(data)
        val dataset = spark.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))

        print("原始特征：")
//        dataset.take(1).foreach(println)
        dataset.take(1).foreach(println)


        //构造切割器
        var slicer = new VectorSlicer()
                .setInputCol("userFeatures")
                .setOutputCol("features")

        //根据索引号，截取原始特征向量的第1列和第3列
        slicer.setIndices(Array(0, 2))
        println("output1: ")
//        slicer.transform(dataset).select("userFeatures", "features").first()
        slicer.transform(dataset).show(false)

        //根据字段名，截取原始特征向量的f2和f3
        slicer = new VectorSlicer()
                .setInputCol("userFeatures")
                .setOutputCol("features")
        slicer.setNames(Array("f2", "f3"))
        println("output2: ")
//        slicer.transform(dataset).select("userFeatures", "features").first()
        slicer.transform(dataset).show(false)


        //索引号和字段名也可以组合使用，截取原始特征向量的第1列和f2
        slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
        slicer.setIndices(Array(0)).setNames(Array("f2"))
        println("output3: ")
//        slicer.transform(dataset).select("userFeatures", "features").first()
        slicer.transform(dataset).show(false)

    }


}
