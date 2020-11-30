package com.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SourceKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    // 指定Kafka的连接位置
    properties.setProperty("bootstrap.servers","yb05:9092")
    // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
    env.addSource(new FlinkKafkaConsumer(
      "flink_kafka",new SimpleStringSchema(),properties
    )).print()
    env.execute()
  }
}
