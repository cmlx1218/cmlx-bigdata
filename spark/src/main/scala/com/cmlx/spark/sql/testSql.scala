package com.cmlx.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/9/25 15:35
 */
object testSql {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.json("")
    df.show()
    df.filter($"age" > 21).show()
    df.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age > 21").show()
    spark.stop()
  }
}
