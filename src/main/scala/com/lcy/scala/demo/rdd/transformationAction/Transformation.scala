package com.lcy.scala.demo.rdd.transformationAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Transformation {
//  val conf = new SparkConf().setAppName("Transformation").setMaster("local")
//  val spark = new SparkContext(conf)

  val spark = SparkSession
    .builder
    .appName("Transformation")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    mapPartitions()
//    mapPartitionsWithIndex()
//    sample()
//    union()
//    intersection()
//    distinct()
//    groupByKey()
//    reduceByKey()
//    aggregateByKey()
//    sortByKey()
//    join()
//    cogroup()
    cartesian()
//    coalesce()
//    repartition()
//    repartitionAndSortWithinPartitions()


  }


  def map()={
    spark.sparkContext.parallelize(1 to 10, 3)
      .map((_ + 1))
      .foreach(println)
  }

  /**
    * filter（function）
    * 过滤操作，满足filter内function函数为true的RDD内所有元素组成一个新的数据集
    * 将一定的数据压缩到更少的partition分区中去
    * 使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
    * filter 算子应用到RDD上面，说白了会应用到RDD对应的里面的每一个partition上去
    * 数据倾斜，换句话说就是可能有的partition里面就剩下了一条数据！
    * 建议用coalesce算子，从而让各个partition中的数据更加紧凑！！
    *
    * coalesce算子，
    */
  def filter(): Unit ={
    spark.sparkContext.parallelize(1 to 10, 1)
      .filter(it => it % 2 == 0)
      .foreach(println)
  }

  /**
    * flatMap（function）
    * map是对RDD中元素逐一进行函数操作映射为另外一个RDD，而flatMap操作是将函数应用于RDD之中
    * 的每一个元素，将返回的迭代器的所有内容构成新的RDD。而flatMap操作是将函数应用于RDD中每一
    * 个元素，将返回的迭代器的所有内容构成RDD。
    *
    * flatMap与map区别在于map为“映射”，而flatMap“先映射，后扁平化”，map对每一次（func）都产生
    * 一个元素，返回一个对象，而flatMap多一步就是将所有对象合并为一个对象。
    */
  def flatMap(): Unit ={
    spark.sparkContext.parallelize(1 to 5, 1)
      .flatMap(( _ to 5))
      .foreach(println)
  }

  /**
    * mapPartitions（function）
    * 区于foreachPartition（属于Action，且无返回值），而mapPartitions可获取返回值。
    * 与map的区别前面已经提到过了，但由于单独运行于RDD的每个分区上（block），
    * 所以在一个类型为T的RDD上运行时，（function）必须是
    * Iterator<T> => Iterator<U>类型的方法（入参）。
    */
  def mapPartitions(): Unit ={
    spark.sparkContext.parallelize(1 to 10, 3)
      .mapPartitions(
        it => {for(e <- it) yield e * 2})
      .foreach(println)
  }

  /**
    * mapPartitionsWithIndex（function）
    * 与mapPartitions类似，但需要提供一个表示分区索引值的整型值作为参数，因此function必
    * 须是（int， Iterator<T>）=>Iterator<U>类型的。
    */
  def mapPartitionsWithIndex(): Unit ={
    spark.sparkContext.parallelize(1 to 10, 3)
      .mapPartitionsWithIndex((x, it) =>{
        val res = List[String]()
        var i =0
        while (it.hasNext){
          i += it.next()
        }
        res.::(x + ":" + i).iterator
      })
      .foreach(println)
  }

  /**
    * sample（withReplacement， fraction， seed）
    * withReplacement是否放回，fraction采样比例，seed用于指定的随机数生成器的种子
    * 是否放回抽样分true和false，fraction取样比例为(0, 1]。seed种子为整型实数。
    */
  def sample(): Unit ={
    spark.sparkContext.parallelize(1 to 10, 3)
      .sample(false, 0.5, 1)
      .foreach(println)
  }

  /**
    * union（otherDataSet）
    * 对于源数据集和其他数据集求并集，不去重
    */
  def union(): Unit ={
    val data = spark.sparkContext.parallelize(1 to 11, 2)
    spark.sparkContext.parallelize(1 to 5, 2)
      .union(data)
      .foreach(println)
  }

  /**
    * intersection（otherDataSet）
    * 对于源数据集和其他数据集求交集，并去重，且无序返回。
    * 会去重
    */
  def intersection(): Unit ={
    val str1 = List("赵敏", "张无忌" , "周芷若", "小昭", "谢逊")
    val str2 = List("赵敏", "张无忌" , "灭绝师太", "圣火令", "倚天剑")
    val data = spark.sparkContext.parallelize(str1, 1)
    spark.sparkContext.parallelize(str2, 1)
      .intersection(data)
      .foreach(println)
  }

  /**
    * distinct（[numTasks]）
    * 返回一个在源数据集去重之后的新数据集，即去重，并局部无序而整体有序返回。
    *
    * 源码
    * def distinct(): RDD[T] = withScope {
    * distinct(partitions.length)
    * }
    * 从源码中看到有Shuffle操作
    */
  def distinct(): Unit ={
    spark.sparkContext.parallelize(List(1,3,5,7,12,34,1,67,12,3,5,12,12,12,7,7,7,7), 2)
      .distinct()
      .collect()
      .foreach(println)
  }

  /**
    * groupByKey([numTasks])
    * groupByKey是将PairRDD中拥有相同key值得元素归为一组
    */
  def groupByKey(): Unit ={
    spark.sparkContext.parallelize(List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "张无忌"), ("峨眉", "周芷若")))
      .groupByKey()
      .foreach(println)
  }

  /**
    * reduceByKey（function，[numTasks]）
    * reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并。
    */
  def reduceByKey(): Unit ={
    spark.sparkContext.parallelize(List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77)))
      .reduceByKey(_+_)
      .foreach(println)
  }

  /**
    * aggregateByKey（zeroValue）（seqOp， combOp， [numTasks]）
    * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
    * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。因为aggregateByKey
    * 是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，对应的结果是Key和
    * 聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。在实现过程中，定义了三个aggregateByKey
    * 函数原型，但最终调用的aggregateByKey函数都一致。
    * 或者
    * 类似reduceByKey，对pairRDD中想用的key值进行聚合操作，使用初始值（seqOp中使用，而combOpenCL中未使用）
    * 对应返回值为pairRDD，而区于aggregate（返回值为非RDD）
    */
  def aggregateByKey(): Unit ={
    spark.sparkContext.parallelize(List("hello world!", "I am a dog", "hello world!", "I am a dog"))
      .flatMap(_.split(" "))
      .map(( _, 1))
      .aggregateByKey(10)(_+_,_+_)
      .foreach(tuple =>println(tuple._1+"->"+tuple._2))
  }

  /**
    * sortByKey（[ascending], [numTasks]）
    *
    */
  def sortByKey(): Unit ={
    spark.sparkContext.parallelize(List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风")))
      .sortByKey(false)
      .foreach(tuple => println(tuple._1 + "->" + tuple._2))
  }

  /**
    * join（otherDataSet，[numTasks]）
    * 加入一个RDD，在一个（k，v）和（k，w）类型的dataSet上调用，返回一个（k，（v，w））的pair dataSet。
    */
  def join(): Unit ={
    val list1RDD = spark.sparkContext.parallelize(
      List((1, List("华山派", "医学")),
          (2, List("武当派", "长生不老")),
          (3, List("明教", "一统江湖")),
          (3, List("崆峒派", "逃跑"))
      ))

    val list2RDD = spark.sparkContext.parallelize(List((1, 66), (2, 77), (3, 88)))

    list1RDD.join(list2RDD)
      .foreach(println)
  }

  /**
    * cogroup（otherDataSet，[numTasks]）
    * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对
    * 两个RDD中相同的key的元素进行合并。
    *
    * 合并两个RDD，生成一个新的RDD。实例中包含两个Iterable值，第一个表示RDD1中相同值，第二个表示RDD2
    * 中相同值（key值），这个操作需要通过partitioner进行重新分区，因此需要执行一次shuffle操作。（
    * 若两个RDD在此之前进行过shuffle，则不需要）
    */

  def cogroup(): Unit ={
    val list1RDD = spark.sparkContext.parallelize(List((1, "cat"), (2, "dog")))
    val list2RDD = spark.sparkContext.parallelize(List((1, "tiger"), (1, "elephant"), (3, "panda"), (3, "chicken")))
    val list3RDD = spark.sparkContext.parallelize(List((1, "duck"), (1, "lion"), (3, "bird"), (3, "fish"), (4, "flowers")))

    list1RDD.cogroup(list2RDD,list3RDD)
      .foreach(println)
  }

  /**
    * cartesian（otherDataSet）
    * 求笛卡尔乘积。该操作不会执行shuffle操作。
    *
    */
  def cartesian(): Unit ={
    val list1RDD = spark.sparkContext.parallelize(List("A","B"))
    val list2RDD = spark.sparkContext.parallelize(List(1,2,3))
    list2RDD.cartesian(list1RDD)
      .foreach(println)
  }

  def pipe(): Unit ={

  }

  /**
    * coalesce（numPartitions）
    * 重新分区，减少RDD中分区的数量到numPartitions。
    *
    */
  def coalesce(): Unit ={
    spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9),3)
      .coalesce(1)
      .foreach(println)
  }

  /**
    * repartition（numPartitions）
    * repartition是coalesce接口中shuffle为true的简易实现，即Reshuffle RDD并随机分区，使各分区数据量
    * 尽可能平衡。若分区之后分区数远大于原分区数，则需要shuffle。
    */
  def repartition(): Unit ={
    spark.sparkContext.parallelize(List(1,2,3,4),1)
      .repartition(2)
      .foreach(println)
  }

  /**
    *.repartitionAndSortWithinPartitions（partitioner）
    * repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，
    * repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
    */
  def repartitionAndSortWithinPartitions(){

    spark.sparkContext.parallelize(List(1, 4, 55, 66, 33, 48, 23),1)
      .map(num => (num,num))
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      .mapPartitionsWithIndex((index,iterator) => {
        val listBuffer: ListBuffer[String] = new ListBuffer
        while (iterator.hasNext) {
          listBuffer.append(index + "_" + iterator.next())
        }
        listBuffer.iterator
      },false)
      .foreach(println)
  }


}
