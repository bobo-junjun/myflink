package com.flink.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

object EventTimeWindows {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    设定事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取socket流
    val datas = env.socketTextStream("yb05", 4545)

    val textKeyStream = datas.map(x => {
      val strings = x.split(" ")
      //      字符串       时间戳            标签，随意写的
      (strings(0), strings(1).toLong, 1)
    })
      //      指定 数据中，哪个是事件时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(1)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = element._2
      })
      .keyBy(0)


    //    textKeyStream.print("key:::")
    //    设定 滚动窗口的 大小， 2s两秒   这个滚动窗口的时间是基于 事件时间进行的统计
    textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))
//      .max(x=>x)
//      .print("window::::")

    env.execute("EventTimeWindows")
  }
}
