//package com.cmlx.spark.function
//
//import org.apache.spark.sql.{Encoder, Encoders}
//import org.apache.spark.sql.expressions.Aggregator
//
//
///**
// * @Author: CMLX
// * @Description: 通过集成Aggregator实现强类型自定义聚合函数
// * @Date: create in 2020/9/28 10:21
// */
//
//// 既然有强类型，可能有case类
//case class Employee(name: String, salary: Long)
//
//case class Average(sum: Long, count: Long)
//
//object MyStrongTypeAverage extends Aggregator[Employee, Average, Double] {
//  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
//  override def zero: Average = Average(0L, 0L)
//
//  override def reduce(buffer: Average, employee: Employee): Average = {
//    buffer.sum += employee.salary
//    buffer.count += 1
//    buffer
//  }
//
//  // 聚合不同execute的结果
//  override def merge(b1: Average, b2: Average): Average = {
//    b1.sum += b2.sum
//    b1.count += b2.count
//    b1
//  }
//
//  // 计算输出
//  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
//
//  // 设定之前值类型的编码器，转换成case
//  // Encoders.product是进行scala元组和case类转换的编码器
//  override def bufferEncoder: Encoder[Average] = Encoders.product
//
//  // 设定最终输出值得编码器
//  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
//}
