package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

/**
 * StandardScaler转换Vector行数据集，将每个要素归一化以具有单位标准差和/或零均值。它带有参数：
 *
 * withStd：默认为True。将数据缩放到单位标准偏差。
 * withMean：默认为False。在缩放之前，将数据以均值居中。它将生成密集的输出，因此在应用于稀疏输入时要小心。
 * StandardScaler是Estimator可以fit在数据集上产生的StandardScalerModel；这相当于计算摘要统计信息。
 * 然后，模型可以转换Vector数据集中的列以具有单位标准差和/或零均值特征。
 *
 * 请注意，如果某个功能的标准偏差为零，它将0.0在该Vector功能的中返回默认值。
 */

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
        scaledData.show()

        spark.stop()
    }
}
