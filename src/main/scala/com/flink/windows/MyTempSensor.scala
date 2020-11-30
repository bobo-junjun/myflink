package com.flink.windows
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random
class MyTempSensor extends RichParallelSourceFunction[MyTempSensor.Sensor] {
  var count: Int = 3
  var running = true

  def this(count: Int) {
    this
    this.count = count
  }

  override def run(sourceContext: SourceFunction.SourceContext[MyTempSensor.Sensor]): Unit = {

    //    生成多个 sensor id，温度
    var num = 0
    //    温度随机生成
    val random = new Random()
    val tuples = 1.to(10).map(i => ("sensor_" + i, random.nextGaussian() * 20))
    while (running && num < count) {
      //      演示 每个传感器的温度变化
      tuples.map(t => (t._1, t._2 + random.nextGaussian())).foreach(x => {
        val times = System.currentTimeMillis()
        sourceContext.collect(MyTempSensor.Sensor(x._1, times, x._2))
        Thread.sleep(100)
      })
      num += 1
    }
  }

  override def cancel(): Unit = running = false
}
object MyTempSensor {

  case class Sensor(sensorId: String, times: Long, temperature: Double)



}