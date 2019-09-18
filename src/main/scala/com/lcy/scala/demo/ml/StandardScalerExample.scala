package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

object StandardScalerExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("StandardScaler")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataFrame = spark
                .read
                .format("libsvm")
                .load("file:\\D:\\sparkData\\sample_libsvm_data.txt")

        val scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                //是否将数据缩放到单位标准差。
                .setWithStd(true)
                //是否在缩放之前使用均值来居中数据。
                //它将构建密集输出，因此在应用稀疏输入时要小心。
                .setWithMean(false)

        // 通过安装StandardScaler计算汇总统计信息。
        val scalerModel = scaler.fit(dataFrame)
        // 将每个特征标准化以具有单位标准偏差。
        val scaledData = scalerModel.transform(dataFrame)
        scaledData.show(false)

        spark.stop()

    }

}


