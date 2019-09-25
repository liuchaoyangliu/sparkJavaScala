package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

/**
 *
 * StringIndexer是指把一组字符型标签编码成一组标签索引，索引的范围为0到标签数量，
 * 索引构建的顺序为标签的频率，优先编码频率较大的标签，所以出现频率最高的标签为0号。
 * 如果输入的是数值型的，我们会把它转化成字符型，然后再对其进行编码。在pipeline组件，
 * 比如Estimator和Transformer中，想要用到字符串索引的标签的话，我们一般需要通过
 * setInputCol来设置输入列。另外，有的时候我们通过一个数据集构建了一个StringIndexer，
 * 然后准备把它应用到另一个数据集上的时候，会遇到新数据集中有一些没有在前一个数据集中出现的标签，
 * 这时候一般有两种策略来处理：第一种是抛出一个异常（默认情况下），第二种是通过掉用
 * setHandleInvalid(“skip”)来彻底忽略包含这类标签的行。
 *
 */


object StringIndexerExample {

    /**
     *
     * 我们首先构建了1个dataframe，然后设置了StringIndexer的输入列和输出列的名字。
     * 通过indexed1.show()，我们可以看到，StringIndexer依次按照出现频率的高低，
     * 把字符标签进行了排序，即出现最多的“a”被编号成0，“c”为1，出现最少的“b”为0。接下来，
     * 我们构建了一个新的dataframe，这个dataframe中有一个再上一个dataframe中未曾
     * 出现的标签“d”，然后我们通过设置setHandleInvalid(“skip”)来忽略标签“d”的行，
     * 结果通过indexed2.show()可以看到，含有标签“d”的行并没有出现。如果，我们没有设置的话，
     * 则会抛出异常，报出“Unseen label: d”的错误
     *
     */

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("StringIndexer")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.createDataFrame(Seq(
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "a"),
            (4, "a"),
            (5, "c"))
        ).toDF("id", "category")

        val indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")

        val indexed = indexer.fit(df).transform(df)
        indexed.show()

        spark.stop()
    }
}
