package com.lcy.scala.demo.rdd.transformationAction

import org.apache.spark.{SparkConf, SparkContext}

object Action {

  val conf = new SparkConf().setAppName("Action").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
//        reduce()
//        collect()
//        count()
//        first()
//        takeSample()
//        take()
//        takeOrdered()
//        saveAsTextFile()
//        saveAsSequenceFile()
//        countByKey()
    groupTop3()
  }

  /**
    * reduce（function）
    * reduce其实是将RDD中的所有元素进行合并，当运行call方法时，会传入两个参数，
    * 在call方法中将两个参数合并后返回，而这个返回值回合一个新的RDD中的元素再次传入call方法中，
    * 继续合并，直到合并到只剩下一个元素时。
    */
  def reduce(): Unit ={
    val result = sc.parallelize(List(1,2,3,4,5,6))
      .reduce((x,y) => x+y)
    println(result)

  }

  /**
    * count（）
    * 将一个RDD以一个Array数组形式返回其中的所有元素。
    */
  def collect(): Unit ={
    val array = sc.parallelize(1 to 10, 2)
      .collect()
    array.foreach(println)
  }

  /**
    * count（）
    * 返回数据集中元素个数，默认Long类型。
    */
  def count(): Unit ={
    println(sc.parallelize(1 to 10, 2).count())
  }

  /**
    * first（）
    * 返回数据集的第一个元素（类似于take(1)）
    */
  def first(): Unit ={
    println(sc.parallelize(1 to 10, 2).first())
  }

  /**
    * takeSample（withReplacement， num， [seed]）
    * 对于一个数据集进行随机抽样，返回一个包含num个随机抽样元素的数组，withReplacement表示
    * 是否有放回抽样，参数seed指定生成随机数的种子。
    * 该方法仅在预期结果数组很小的情况下使用，因为所有数据都被加载到driver端的内存中。
    */
  def takeSample(): Unit ={
    val array = sc.parallelize(1 to 100, 2).takeSample(true,3,1)
    array.foreach(println)
  }

  /**
    * take（n）
    * 返回一个包含数据集前n个元素的数组（从0下标到n-1下标的元素），不排序。
    */
  def take(): Unit ={
    val array = sc.parallelize(List(2,7,1,8,3),2).take(3)
    array.foreach(println)
  }

  /**
    * takeOrdered（n，[ordering]）
    * 返回RDD中前n个元素，并按默认顺序排序（升序）或者按自定义比较器顺序排序。
    */
  def takeOrdered(): Unit ={
    val array = sc.parallelize(List(2,7,1,8,3),2).takeOrdered(3)
    array.foreach(println)
  }

  /**
    * saveAsTextFile（path）
    * 将dataSet中元素以文本文件的形式写入本地文件系统或者HDFS等。Spark将对每个元素调用toString方法，
    * 将数据元素转换为文本文件中的一行记录。
    * 若将文件保存到本地文件系统，那么只会保存在executor所在机器的本地目录。
    */
  def saveAsTextFile(): Unit ={
    sc.parallelize(List(2,7,1,8,3),1)
      .map(v => v+2)
      .saveAsTextFile("E:\\data\\")
  }

  /**
    * saveAsSequenceFile（path）（Java and Scala）
    * 将dataSet中元素以Hadoop SequenceFile的形式写入本地文件系统或者HDFS等。（对pairRDD操作）
    */
  def saveAsSequenceFile(): Unit ={
    sc.parallelize(List(2,7,1,8,3),2).saveAsObjectFile("E:\\data\\")
  }

  /**
    * countByKey（）
    * 用于统计RDD[K,V]中每个K的数量，返回具有每个key的计数的（k，int）pairs的hashMap。
    */
  def countByKey(): Unit ={
    sc.textFile("file:\\E:\\sparkData\\demo.txt", 2)
      .flatMap(line => line.split(","))
      .map(str => (str, 1))
      .countByKey()
      .foreach(println)
  }

  /**
    * 数据
    * class2,1
    * class3,2
    * class3,11
    * class4,12
    * class2,13
    * class3,14
    * class4,15
    * class3,5
    * class4,6
    * class2,7
    * class4,3
    * class2,4
    * class3,8
    * class4,9
    * class2,10
    */



  /**
    * foreach（function）
    */
  def foreach(): Unit ={

  }


  def groupTop3(): Unit ={

    sc.textFile("file:\\D:\\sparkData\\words.txt")
      .map(line => {
        val datas = line.split(" ")
        (datas(0), datas(1))
      })
      .groupByKey()
      .map(group => (group._1, group._2.toList.sortWith(_ > _).take(3)))
      .sortByKey()
      .foreach(group => {
        println(group._1)
        group._2.foreach(println)
      })

    sc.stop()

  }


}
