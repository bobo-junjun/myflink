package com.flink.source

import org.apache.flink.streaming.api.scala._

object DataSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
 // 1.  readTestFile测试  失败 没有出结果
//    env.readTextFile("data/wc.txt").print("readTextFilec测试")

    //2. readFile测试   转去java代码测试
//    env.readFile(
//      new TextInputFormat(new Path("data/wc.txt"))
//    )

  }
}
