//package com.cmlx.spark.function
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
///**
// * @Author: CMLX
// * @Description:
// * @Date: create in 2020/9/28 10:32
// */
//object testMyStrongTypeAverage {
//
//  def main(args: Array[String]): Unit = {
//    //测试环境使用一个内核即可，生产环境中进行修改
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("testMyAverage")
//
//    //创建SparkSession
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//
//    import spark.implicits._
//
//    val ds = spark.read.json("examples/src/main/resources/employees.json").as[Employee]
//    ds.show()
//    // +-------+------+
//    // |   name|salary|
//    // +-------+------+
//    // |Michael|  3000|
//    // |   Andy|  4500|
//    // | Justin|  3500|
//    // |  Berta|  4000|
//    // +-------+------+
//
//    // Convert the function to a `TypedColumn` and give it a name
//    val averageSalary = MyStrongTypeAverage.toColumn.name("average_salary")
//    val result = ds.select(averageSalary)
//    result.show()
//    // +--------------+
//    // |average_salary|
//    // +--------------+
//    // |        3750.0|
//    // +--------------+
//  }
//
//}
