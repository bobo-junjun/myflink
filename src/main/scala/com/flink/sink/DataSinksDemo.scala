package com.flink.sink

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

object DataSinksDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //writeAsText
    val value3 = env.fromElements(1, 2, 3)
    value3.writeAsText("./data/writeAsText.txt",FileSystem.WriteMode.OVERWRITE)
      env.execute("writeAsText")

  }
}
