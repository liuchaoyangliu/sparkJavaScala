package com.lcy.scala.demo.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

object EstimatorTransformerParamExample2 {

    def main(args: Array[String]) {

        val spark = SparkSession
                .builder
                .appName(s"LogisticRegressionExample")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        //把lines转换为适合LogisticRegression算法输入格式的dataframe，也就是原始的四列数据转换成两列，两列的类型是Double,Vector，
        //列名是"label", "features"，它是LogisticRegression算法默认的输入列名，这里必须写这两个名称（除非改掉默认值）
        val training = spark
                .sparkContext
                .textFile("file:/home/ubuntu/sparkData/dataFrame.txt")
                .map(line => {
                    val strings = line.split(",")
                    (
                            strings(0).toDouble,
                            Vectors.dense(
                                strings(1).toDouble,
                                strings(2).toDouble,
                                strings(3).toDouble
                            )
                    )
                })
                .toDF("label", "features")


        // 创建一个 LogisticRegression 实例
        val lr = new LogisticRegression()

        // 输出LogisticRegression这个算法的所有参数
        println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

        /**
         * 结果：
         * *
         * LogisticRegression parameters:
         * 逻辑回归参数：
         * *
         * AggregationDepth:建议的TreeAgregate深度（>=2）（默认值：2）
         * ElasticNetParam：范围[0，1]内的ElasticNet混合参数。对于alpha=0，惩罚为l2惩罚。对于alpha=1，它是一个l1惩罚（默认值：0.0）
         * 族：族的名称，是对模型中要使用的标签分布的描述。支持选项：自动、二项式、多项式。（默认：自动）
         * FeaturesCol:功能列名称（默认：features）
         * FitIntercept：是否符合拦截项（默认值：true）
         * labelcol:标签列名称（默认：label）
         * lowerBoundsOnCoefficients：在约束优化下拟合时系数的下界。（未定义）
         * LowerboundsonIntercepts：在约束优化下拟合截取的下限。（未定义）
         * maxIter：最大迭代次数（>=0）（默认值：100）
         * predictionCol:预测列名称（默认值：prediction）
         * probabilityCol：预测类条件概率的列名称。注：并非所有模型都输出经过校准的概率估计值！
         * 这些概率应被视为信任，而不是精确的概率（默认值：probability）
         * rawPredictionCol:原始预测（a.k.a.置信度）列名称（默认值：rawPrediction）
         * regparam:正则化参数（>=0）（默认值：0.0）
         * standardization：是否在拟合模型前标准化培训功能（默认值：true）
         * threshold：二进制分类预测中的阈值，范围[0，1]（默认值：0.5）
         * thresholds：多类分类中的阈值，用于调整预测每个类的概率。数组的长度必须等于类的数目，值大于0，但最多只能有一个值为0。
         * 预测P/T值最大的类，其中P是该类的原始概率，T是类的阈值（未定义）
         * tol：迭代算法的收敛公差（>=0）（默认值：1.0e-6）
         * upperBoundsOnCoefficients：在约束优化下拟合时系数的上界。（未定义）
         * upperboundsonIntercepts：截获上的上界，如果符合下界约束优化。（未定义）
         * weightcol：权重列名称。如果未设置或为空，则将所有实例权重视为1.0（未定义）
         *
         */

        //设置算法参数
        lr.setMaxIter(10).setRegParam(0.01)

        // 训练出一个模型model1
        val model1 = lr.fit(training)

        // 输出model1所使用的算法参数，比如上面的MaxIter是10
        println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

        //也可以通过ParamMap这个集合来预设置好算法参数，后面算法想要设置参数时就可以直接使用这个集合。
        val paramMap = ParamMap(lr.maxIter -> 20)
                .put(lr.maxIter, 30)
                .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

        // 也可以修改默认的输出参数列名比如把probilityCol修改为"myProbability"（probabilityCol默认名为"probability"）
        //注意"myProbability"指的是算法输出的参数名称，不是输入参数的名称ba
        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
        val paramMapCombined = paramMap ++ paramMap2

        //用新设置的参数来训练出一个模型model2
        val model2 = lr.fit(training, paramMapCombined)
        //输出model2所使用的算法参数
        println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

        // 准备好测试数据，这里我们通过接口自定义一份dataframe（test)
        //toDF("label", "features")，是自定义的列名，因为LogisticRegression 默认的输入参数名为"label", "features"，
        // 所以这里设置要和默认的一样。类型是（double,Vector)。
        //val model2 = lr.fit(training, paramMapCombined).set(lr.labelCol, "mylabel"),
        // model2如果把默认的lr.labelCol值改为"mylabel"，
        //那么这里的toDF("label", "features")要改为toDF("mylabel", "features")。
        //.set(lr.labelCol, "mylabel")这样是改算法输入参数的名称和模型预测结果的参数名称，注意和上面修改probabilityCol的区别。
        //大家自己改下这些参数，体验一下就明白了

        val test = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
            (0.0, Vectors.dense(3.0, 2.0, -0.1)),
            (1.0, Vectors.dense(0.0, 2.2, -1.5))
        )).toDF("label", "features")

        //用model2来预测数据集test
        val r = model2.transform(test)

        //输出结果的元数据，可以看到probabilityCol为"myProbability"，这里就是上面所说的输出参数的名称。
        println(r.schema)

        //输出预测的结果
        r.collect().foreach(println)

        //通过参数名来获取指定的结果，即输出我们自己想要的结果。
        //结果主要看三列"features", "label","prediction"
        //"features"指原始数据，"label"指原始数据的真实类别，"prediction"指原始数据的预测结果。
        r.select("features", "label", "rawPrediction", "myProbability", "prediction")
                .collect()
                .foreach { case Row(feature: Vector, label: Double, pre: Vector, prob: Vector, prediction: Double) =>
                    println(s"label=$label ,feature=$feature, rawPrediction=$pre,probability=$prob, prediction=$prediction")
                }
    }

}


