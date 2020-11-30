package com.flink.introduction

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink的流处理练习
 */
object WCStreaming {
  def main(args: Array[String]): Unit = {
    //使用StreamExecutionEnvironment的形式来获取流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //这是使用socketTextStream的方式来测试
    val dataStream = env.socketTextStream("192.168.8.105", 1234, '\n')
    dataStream.flatMap(_.toLowerCase().split("[\n ,]"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)
      .print()
    env.execute("WCStreaming")
  }
}
