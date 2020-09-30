package com.cmlx.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/9/28 20:14
 */
object KafkaDataSource {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志 ,在终端上显示需要的日志
    //1、初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2、初始化SparkStreamingContext
    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //  val kafkaParams = Map(
    //    "metadata.broker.list" -> "master:9092",
    //    "group.id" -> "group_hgs",
    //    "zookeeper.connect" -> "master:2181"
    //  )
    //
    //  val topics = Set[String]("test")
    //  //3、从kafka采集数据
    //  KafkaUtils.createDirectStream(ssc,kafkaParams,topics)
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "master:2181",
      "cmlx",
      Map("cmlx" -> 3)
    )

    //4、将一行数据切分，形成一个个单词(扁平化)
    val wordStreams = kafkaDStream.flatMap(_._2.split(" "))

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
