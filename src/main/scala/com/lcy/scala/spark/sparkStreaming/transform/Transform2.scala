package com.lcy.scala.spark.sparkStreaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform2 {

  def main(args: Array[String]): Unit = {

    /**

    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformaDemo")
    val  ssc = new StreamingContext(conf, Seconds(5))

    val wordCountDS = ssc.socketTextStream("192.168.75.132", 9999)
      .flatMap(line => line.split(" "))
      .map(word => (word,1))

    val filterData = ssc.sparkContext.parallelize(List(",","?","!","."))
      .map(param => (param,true))

    val wcDS = wordCountDS.transform( rdd =>{
      rdd.leftOuterJoin(filterData)
        .filter(tuple =>{
          if(tuple._2._2.isEmpty){
            true
          }else{
            false
          }
        })
        .map(tuple =>(tuple._1,1))

    })
      .reduceByKey(_+_)

    wcDS.print()
    ssc.start()
    ssc.awaitTermination()

      */

    val conf = new SparkConf().setAppName("TransForm").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val filterData = ssc
      .sparkContext
      .parallelize(List(",", "?", "!", "."))
      .map(e => (e, true))

    ssc.socketTextStream("local", 9999)
      .flatMap(line => line.split(" "))
      .map(e => (e, 1))
      .transform(rdd => {
        rdd.leftOuterJoin(filterData)
          .filter(tuple => if(tuple._2._2.isEmpty) true else false)
          .map(tuple => (tuple._1, 1))
      })
      .reduceByKey(_+_)
      .print()


    ssc.start()
    ssc.awaitTermination()

  }

}
