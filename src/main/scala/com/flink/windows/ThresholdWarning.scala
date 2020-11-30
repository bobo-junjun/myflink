package com.flink.windows

import java.util

import org.apache.commons.compress.utils.Lists
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 预警系统
class ThresholdWarning extends RichFlatMapFunction[(String, Long), (String, util.ArrayList[Long])] {
  var abnormalData: ListState[Long] = _
  var threshold: Long = 3
  var numberOfTimes: Int = 3

  //  设定state的实效时间，及更新状态state的方式
  val ttlConfig: StateTtlConfig = StateTtlConfig
    .newBuilder(Time.seconds(5))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()
  def this(threshold: Long, numberOfTimes: Int) {
    this
    this.threshold = threshold
    this.numberOfTimes = numberOfTimes
  }

  override def open(parameters: Configuration): Unit = {
    val abnormalDescriptor = new ListStateDescriptor[Long]("abnormalData", classOf[Long])
    //    将设置的 state ttl放入 liststate中
    abnormalDescriptor.enableTimeToLive(ttlConfig)
    abnormalData = getRuntimeContext.getListState(abnormalDescriptor)
  }

  override def flatMap(value: (String, Long), out: Collector[(String, util.ArrayList[Long])]): Unit = {
    if (value._2 >= threshold) {
      abnormalData.add(value._2)
    }
    val longs = Lists.newArrayList(abnormalData.get().iterator())
    if (longs.size() >= numberOfTimes) {
      out.collect(value._1, longs) // 假如 预警信息，操作指定数量，将其输出
      abnormalData.clear()
    }

  }
}

object ThresholdWarning {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val datas = env.fromElements(
      ("a", 50L), ("a", 80L), ("a", 400L),
      ("a", 100L), ("a", 200L), ("a", 200L),
      ("b", 100L), ("b", 200L), ("b", 200L),
      ("b", 500L), ("b", 600L), ("b", 700L))
    datas.keyBy(0).flatMap(new ThresholdWarning(400L,3))
      .printToErr()



    env.execute("ThresholdWarning")
  }
}
