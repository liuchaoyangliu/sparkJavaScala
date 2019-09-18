package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object StringIndexerExample2 {

    /**
     *
     * 我们首先构建了1个dataframe，然后设置了StringIndexer的输入列和输出列的名字。
     * 通过indexed1.show()，我们可以看到，StringIndexer依次按照出现频率的高低，
     * 把字符标签进行了排序，即出现最多的“a”被编号成0，“c”为1，出现最少的“b”为0。接下来，
     * 我们构建了一个新的dataframe，这个dataframe中有一个再上一个dataframe中未
     * 曾出现的标签“d”，然后我们通过设置setHandleInvalid(“skip”)来忽略标签“d”的行，
     * 结果通过indexed2.show()可以看到，含有标签“d”的行并没有出现。如果，
     * 我们没有设置的话，则会抛出异常，报出“Unseen label: d”的错误
     *
     */

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("stringIndex")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val df1 = spark.createDataFrame(
            Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
        ).toDF("id", "category")


        val indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")

        indexer.fit(df1).transform(df1).show(false)

        val df2 = spark.createDataFrame(
            Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "d"))
        ).toDF("id", "category")

        try {
            indexer.fit(df1).transform(df2).show(false)
        } catch {
            case e: Exception => print(e)
        }

        indexer
                .fit(df1)
                .setHandleInvalid("skip")
                .transform(df2)
                .show(false)

        indexer
                .fit(df1)
                .setHandleInvalid("keep")
                .transform(df2)
                .show(false)

        spark.stop()

    }


}
