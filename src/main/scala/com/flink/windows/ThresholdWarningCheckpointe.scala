package com.flink.windows

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._ //  在scala使用java的类型，导入这个包


// 预警系统
class ThresholdWarningCheckpointe extends RichFlatMapFunction[(String, Long), (String, util.ArrayList[(String, Long)])] with CheckpointedFunction {
  var threshold: Long = 3
  var numberOfTimes: Int = 3
  var bufferedData: util.ArrayList[(String, Long)] = _
  var checkPointedState: ListState[(String, Long)] = _

  //  设定state的实效时间，及更新状态state的方式
  val ttlConfig: StateTtlConfig = StateTtlConfig
    .newBuilder(Time.seconds(5))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()

  def this(threshold: Long, numberOfTimes: Int) {
    this
    this.threshold = threshold
    this.numberOfTimes = numberOfTimes
    this.bufferedData = new util.ArrayList[(String, Long)]()
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkPointedState.clear()
    for (a <- bufferedData) {
      checkPointedState.add(a)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    //    val abnormalData: ListStateDescriptor[String] = new ListStateDescriptor("abnormalData", TypeInformation.of(new TypeHint[String, Long] {}))
    checkPointedState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[(String, Long)]("abnormalData", TypeInformation.of(new TypeHint[(String, Long)] {})))

    if (context.isRestored) {
      while (checkPointedState.get().iterator().hasNext) {
        val tuple: (String, Long) = checkPointedState.get().iterator().next()
        bufferedData.add(tuple)
      }
    }
  }

  override def flatMap(value: (String, Long), out: Collector[(String, util.ArrayList[(String, Long)])]): Unit = {
    if (value._2 >= threshold) {
      //      println(value._2)
      bufferedData.add(value)
    }
    if (bufferedData.size() >= numberOfTimes) {
      out.collect((checkPointedState.hashCode() + "阈值警报！", bufferedData)) // 假如 预警信息，操作指定数量，将其输出
      bufferedData.clear()
    }
  }

}

object ThresholdWarningCheckpointe {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.setParallelism(1)
    val datas = env.fromElements(
      ("a", 50L), ("a", 80L), ("a", 400L),
      ("a", 100L), ("a", 200L), ("a", 200L),
      ("b", 100L), ("b", 200L), ("b", 200L),
      ("b", 500L), ("b", 600L), ("b", 700L))
    datas.flatMap(new ThresholdWarningCheckpointe(400L, 3))
      .printToErr()
    env.execute("ThresholdWarningCheckpointe")
  }
}
