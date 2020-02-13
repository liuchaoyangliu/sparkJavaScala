package com.lcy.scala.spark.sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * 自定义接收器
 *
 * 未看懂
 */
object CustomReceiver {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("CustomReceiver")
        val ssc = new StreamingContext(sparkConf, Seconds(1))

        //使用目标ip:port上的自定义接收器创建一个输入流，并计算
        //以\n分隔的文本的输入流中的单词(例如。所产生的“数控”)
        val lines = ssc.receiverStream(new CustomReceiver("192.168.75.132", 9999))
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()


        ssc.start()
        ssc.awaitTermination()

    }
}


class CustomReceiver(host: String, port: Int)
        extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

    def onStart() {
        // 启动通过连接接收数据的线程
        new Thread("Socket Receiver") {
            override def run() {
                receive()
            }
        }.start()
    }

    def onStop() {
        //因为调用receive()的线程本身被设计为stop isStopped()返回false，所以不需要做太多工作
    }

    /** 创建套接字连接并接收数据，直到接收器停止 */
    private def receive() {
        var socket: Socket = null
        var userInput: String = null

        try {
            logInfo(s"Connecting to $host : $port")
            socket = new Socket(host, port)
            logInfo(s"Connected to $host : $port")
            val reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
            userInput = reader.readLine()
            while (!isStopped && userInput != null) {
                store(userInput)
                userInput = reader.readLine()
            }
            reader.close()
            socket.close()
            logInfo("Stopped receiving")
            restart("Trying to connect again")
        } catch {
            case e: java.net.ConnectException =>
                restart(s"Error connecting to $host : $port", e)
            case t: Throwable =>
                restart("Error receiving data", t)
        }

    }

}
