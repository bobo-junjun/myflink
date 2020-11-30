package com.flink.windows
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

object EventTimeWindowsSentor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    设定事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)
    val datas = env.addSource(new MyTempSensor(30))
    val textKeyStream = datas.map(x => {
      (x.sensorId, x.times, x.temperature)
    })
      //      指定 数据中，哪个是事件时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Double)](Time.seconds(1)) {
        override def extractTimestamp(element: (String, Long, Double)): Long = element._2
      })
      .keyBy(0)

    //    textKeyStream.print("key:::")
    //    设定 滚动窗口的 大小， 2s两秒   这个滚动窗口的时间是基于 事件时间进行的统计
    textKeyStream
      //      .window(TumblingEventTimeWindows.of(Time.seconds(6)))   //滚动窗口的 测试
      //      .window(TumblingEventTimeWindows.of(Time.seconds(6),Time.seconds(2)))  //滑动窗口
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(5000))) //会话窗口
      .reduce((a, b) => {
        (a._1 + a._2 + "--" + b._2, 0L, a._3 + b._3)
      })
      .map(_._1)
      //      滑动窗口和 滚动窗口用的是 fold
      /*.fold("events") {
        case (set, (key, ts, temperature)) =>
          set + "-" + ts
          //          s"${key},${a},${temperature}"
      }*/

      .print("window::::").setParallelism(1)

    env.execute("EventTimeWindows")
  }
}