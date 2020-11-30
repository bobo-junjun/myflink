package com.flink.windows

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object FlinkWindowsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val dataStreams = env.socketTextStream("yb05", 4545)
    dataStreams.flatMap(new RichFlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings = value.split(" ")
        for (s <- strings) {
          out.collect((s, 1L))
        }
      }
    }).keyBy(0)
          .timeWindow(Time.seconds(3))   //   滚动窗口，数据没有重叠
        .sum(1).print()
    env.execute("timeWindow")
  }
}
