package com.flink.transformation

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctionsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val value3 = env.fromElements(1, 2, 3)
      value3.flatMap(new MyFlatMap).print()
    env.execute("RichFunctionsDemo")
  }
}
class MyFlatMap extends RichFlatMapFunction[Int,(Int,Int)]{
  var subTaskIndex = 0
  override def open(parameters: Configuration): Unit = subTaskIndex = getRuntimeContext.getIndexOfThisSubtask


  override def flatMap(in: Int, collector: Collector[(Int, Int)]): Unit = {
          if( in % 2 == subTaskIndex){
            collector.collect((subTaskIndex,in))
          }
  }
  override def close(): Unit = super.close()
}