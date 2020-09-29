package com.cmlx.spark.dstream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: CMLX
 * @Description: 自定义数据源 ----> 也叫自定义采集器
 *               继承Receiver，并实现onStart和onStop方法来自定义采集器
 * @Date: create in 2020/9/28 11:12
 */
object MyReceiver {

  def main(args: Array[String]): Unit = {

    //请注意是apache.log4j不是org.slf4j


    // 屏蔽不必要的日志 ,在终端上显示需要的日志
    //1、初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2、初始化SparkStreamingContext
    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3、通过监控端口创建DStream，读进来的数据为一行行
    //    val socketLineDStream:ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)
    //    val fileDStream: DStream[String] = ssc.textFileStream("test/file")
    val receiverDStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("master", 9999))
    //4、将一行数据切分，形成一个个单词(扁平化)
    val wordStreams = receiverDStream.flatMap(_.split(" "))

    //5、将单词映射成元祖(word,1)
    val wordAndOneStreams = wordStreams.map((_, 1))

    //6、将相同单词做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_ + _)

    //7、打印
    wordAndCountStreams.print()

    //8、启动SparkStreamingContext，不能停止采集工作
    ssc.start()
    ssc.awaitTermination()
  }

  // 声明采集器
  // 1)继承Receiver
  // 2)重写方法 onStart onStop
  class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    var socket: Socket = null

    def receiver(): Unit = {
      socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

      var line: String = null

      while ((line = reader.readLine()) != null) {
        // 将采集的数据存储到采集器内部进行转换
        if ("END".equals(line)) {
          return
        } else {
          this.store(line)
        }
      }
    }

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          receiver()
        }
      }).start()
    }

    override def onStop(): Unit = {
      if (socket != null) {
        socket.close()
      }
    }
  }
}