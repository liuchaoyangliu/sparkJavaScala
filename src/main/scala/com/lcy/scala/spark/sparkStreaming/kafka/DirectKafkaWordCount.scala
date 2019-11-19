package com.lcy.scala.spark.sparkStreaming.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DirectKafkaWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DirectKafkaWordCount")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://ambari.master.com:8020/spark/dkl/kafka/wordcount_checkpoint")

    val server = "ambari.master.com:6667"

    //配置消费者
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server, //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "UpdateStateBykeyWordCount", //消费者组名
      "auto.offset.reset" -> "latest", //latest自动重置偏移量为最新的偏移量   earliest 、none
      "enable.auto.commit" -> (false: java.lang.Boolean)) //如果是true，则这个消费者的偏移量会在后台自动提交

    val topics = Array("UpdateStateBykeyWordCount") //消费主题

    //基于Direct方式创建DStream
    val stream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.flatMap(_.value().split(" "))
      .map((_, 1))
      .updateStateByKey(
        (values: Seq[Int], state: Option[Int]) => {
          var newValue = state.getOrElse(0)
          values.foreach(newValue += _)
          Option(newValue)
        })
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
