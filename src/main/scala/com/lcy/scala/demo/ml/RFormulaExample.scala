package com.lcy.scala.demo.ml

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

/**
 *
 * RFormula选择由R模型公式指定的列。目前，我们支持R运算符的有限子集，包括'〜'，'。'，'：'，'+'和' - '。
 * 基本操作符是：
 * ~ 单独的目标和条款
 * + concat术语，“+ 0”表示删除拦截
 *  - 删除一个术语，“ - 1”表示删除拦截
 * : 交互（数值乘法或二值化分类值）
 * . 除目标之外的所有列
 *
 * 假设a并且b是双列，我们使用以下简单示例来说明以下效果RFormula：
 * y ~ a + b表示模型y ~ w0 + w1 * a + w2 * b，其中w0是截距，w1, w2是系数。
 * y ~ a + b + a:b - 1表示模型y ~ w1 * a + w2 * b + w3 * a * b在哪里w1, w2, w3是系数。
 *
 * RFormula生成一个特征向量列和一个标签的双列或字符串列。就像在R中使用公式进行线性回归一样，
 * 字符串输入列将是单热编码的，而数字列将被转换为双精度。如果label列的类型为string，
 * 则首先将其转换为double StringIndexer。如果DataFrame中不存在label列，
 * 则将从公式中的指定响应变量创建输出标签列。
 *
 */

object RFormulaExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder
                .appName("RFormula")
                .master("local")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val dataset = spark.createDataFrame(Seq(
            (7, "US", 18, 1.0),
            (8, "CA", 12, 0.0),
            (9, "NZ", 15, 0.0)
        )).toDF("id", "country", "hour", "clicked")

        val formula = new RFormula()
                .setFormula("clicked ~ country + hour")
                .setFeaturesCol("features")
                .setLabelCol("label")

        val output = formula
                .fit(dataset)
                .transform(dataset)
        output.show(false)
        //        output.select("features", "label").show()

        spark.stop()

    }

}
