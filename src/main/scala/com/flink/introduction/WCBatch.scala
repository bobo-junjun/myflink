package com.flink.introduction

import org.apache.flink.api.scala._

object WCBatch {
  def main(args: Array[String]): Unit = {
//      获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
//    读取source文件  返回的是DataSet
val dataSet: DataSet[String] = env.readTextFile("data/wc.txt")
    dataSet.flatMap(_.toLowerCase().split(" "))
//    dataSet.map(_.toLowerCase().split(" "))    //使用map会报错This type (BasicArrayTypeInfo<String>) cannot be used as key.
      .filter(_.nonEmpty)
      .map((_,1))           //返回一个元组  key是元素  value是计数
      .groupBy(0)   //根据key分组
      .sum(1)       //统计每个组内的数量
      .print()

  }
}
