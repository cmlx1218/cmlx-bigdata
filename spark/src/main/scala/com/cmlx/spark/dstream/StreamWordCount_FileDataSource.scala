package com.cmlx.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: CMLX
 * @Description: DStreamQ案例操作--WordCount
 *               使用netcat工具向9999端口不断发送数据，通过SparkStreaming读取端口数据并统计不同单词出现的次数
 *               启动项目后到服务器：nc -lk 9999
 *               hello cmlx
 *               如果日志太多可以将log4j文件日志级别改为WARN
 * @Date: create in 2020/9/28 11:12
 */
object StreamWordCount_FileDataSource {

  def main(args: Array[String]): Unit = {

    //请注意是apache.log4j不是org.slf4j
    import org.apache.log4j.{Level, Logger}


    // 屏蔽不必要的日志 ,在终端上显示需要的日志
    //1、初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2、初始化SparkStreamingContext
    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3、通过监控端口创建DStream，读进来的数据为一行行
    //    val socketLineDStream:ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)
    val fileDStream: DStream[String] = ssc.textFileStream("test/file")

    //4、将一行数据切分，形成一个个单词(扁平化)
    val wordStreams = fileDStream.flatMap(_.split(" "))

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

}
