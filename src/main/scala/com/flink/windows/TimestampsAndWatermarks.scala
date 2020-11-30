package com.flink.windows

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[MyTempSensor.Sensor](Time.seconds(5)) {
  override def extractTimestamp(element: MyTempSensor.Sensor): Long = element.times

}

object TimestampsAndWatermarks {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.addSource(new MyTempSensor).assignTimestampsAndWatermarks(new SensorTimeAssigner).print()
    env.execute("TimestampsAndWatermarks")
  }
}

