package com.cmlx.spark.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/9/27 17:57
 */
object testMyAverage {

  def main(args: Array[String]): Unit = {


    //测试环境使用一个内核即可，生产环境中进行修改

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("testMyAverage")

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //  val spark:SparkContext = sparkSession.sparkContext

    //  val spark = SparkSession.builder()
    //    .master("local")
    //    .appName("SparkSessionApp")
    //    .getOrCreate()

    // 注册函数
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+

  }
}
