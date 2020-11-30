package com.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object DefineDataSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //如果你想要实现具有并行度的输入流，则需要实现 ParallelSourceFunction
    // 或 RichParallelSourceFunction 接口
    env.addSource(new SourceFunction[Long] {
    var count = 0L
    var isRunning = true
      override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
        while (isRunning && count < 100){
          sourceContext.collect(count)
          count += 1
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = isRunning = false   //running等于false的时候取消
    }).print("DefineDataSource")
    env.execute()
  }
}
