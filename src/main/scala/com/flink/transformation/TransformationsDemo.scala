package com.flink.transformation

import org.apache.flink.streaming.api.scala._

object TransformationsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //map
    //      env.fromElements(1,2,3).map(_*2).print()

    //FlatMap
//    var  string01 = "one one one two two"
//    var string02 = "third third third four"
//    env.fromElements(string01, string02)
//      .flatMap(_.split(" ")).print()
//        env.execute()


   env.readTextFile("data/minby.txt")
      .map(x=>{
        val strings = x.toLowerCase.split(" ")
        (strings(0),strings(1).toInt)
      })
      //      .filter(_.nonEmpty)
      //      .map(x => (x(0), x(1)))
      .keyBy(0)
      //      .reduce((x,y)=>{
      //        (x._1,x._2+y._2)
      //      })
      //      .sum(1)  // 将第二个字段的值进行累加
      .minBy(1)
      .print()
    env.execute()


  }
}
