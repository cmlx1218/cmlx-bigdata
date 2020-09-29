package com.cmlx.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: CMLX
 * @Description: 统计出每一个省份广告被点击次数的TOP3
 * @Date: create in 2020/9/24 16:36
 */
object testRDD {

  def main(args: Array[String]): Unit = {

    //1、初始化spark配置信息并建立与spark的连接
//    val sparkConf = new SparkConf()
//      // 设置yarn-client模式提交
//      .setMaster("yarn")
////      .set()
//      .set("spark.yarn.jars","hdfs://kason-pc:9000/system/spark/yarn/jars/*")
//      .setJars(List("/home/kason/workspace/BigdataComponents/out/artifacts/SparkLearn_jar/SparkLearn.jar"))//.setJars(GETJars.getJars("/home/kason/workspace/BigdataComponents/spark-main/target/spark-main/WEB-INF/lib"))

    val sparkConf = new SparkConf()
      .setAppName("testRDD")
      .setMaster("yarn")
      .set("deploy-mode","cluster")




    //      .setMaster("yarn")
//      .setAppName("Practice")
//      .set("spark.hadoop.yarn.resourcemanager.hostname","master")
//      .set("spark.hadoop.yarn.resourcemanager.address","master:8050")
//      .set("spark.driver.extraJavaOptions","-Dhdp.version=3.1.4.0-315")
//      .set("spark.yarn.am.extraJavaOptions","-Dhdp.version=3.1.4.0-315")
    val sc = new SparkContext(sparkConf)

    //2、读取数据生产RDD：TS,Province,City,User,AD
    val lines: RDD[String] = sc.textFile("D:\\project\\document\\test\\agent.txt")

    //3、按照最小粒度聚合((Province,AD),1)
    val provinceAdToOne:RDD[((String, String), Int)] = lines.map {
      line => {
        val fields = line.split(" ")
        ((fields(1), fields(4)), 1)
      }
    }

    //4、计算每个省中每个广告被点击的次数((Province,AD),sum)
    val provinceAdToSum:RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_+_)

    //5、将省份作为key，广告加点击数为value(Province,(AD,sum))
    val provinceToAdSum:RDD[(String, (String, Int))] = provinceAdToSum.map(x => (x._1._1,(x._1._2,x._2)))

    //6、将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provincesGroup:RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()

    //7、对同一个省份的广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceADTop3:RDD[(String, Iterable[(String, Int)])] = provincesGroup.mapValues{
      x =>{
        x.toList.sortWith((x,y) => x._2 > y._2).take(3)
      }
    }

    //8、将数据拉取到Driver端并打印
    provinceADTop3.collect().foreach(println(_))

    //9、关闭与spark的连接
    sc.stop()

  }

}
