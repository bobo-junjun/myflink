package com.flink.transformation

import org.apache.flink.streaming.api.scala._

object TransformationDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //Union 用于连接两个或者多个元素类型相同的 DataStream
    val value1 = env.fromElements(1, 2, 3)
    val value2 = env.fromElements(10, 20, 30)
    //    value1.union(value2).print()
    //    env.execute("Union")

    //Connect Connect 操作用于连接两个或者多个类型不同的 DataStream ，其返回的类型是
    // ConnectedStreams ，此时被连接的多个 DataStreams
    // 可以共享彼此之间的数据状态。但是需要注意的是由于不同 DataStream 之间的数据类型是不同的，
    // 如果想要进行后续的计算操作，还需要通过 CoMap 或 CoFlatMap 将 ConnectedStreams 转换回

    //    value1.connect(value2).map(x=>println(x),y=>println(y))
    //    env.execute("connect")


    //Split 和 Select
    value1.split(x => (x == 2) match {
      case true => List("hadoop")
      case false => List("other")
    })
      .select("other")
      .print()
    env.execute("Split")

    //Custom partitioningFlink 运行用户采用自定义的分区规则来实现分区，此时需要通过实现
    // Partitioner 接口来自定义分区规则，并指定对应的分区键
    // 这个只能用java测试


  }
}
