package com.flink.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.environment._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.apache.kafka.common.metrics.Sensor
import org.elasticsearch.client.Requests

/**
 * 测试es
 */
object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val datas=env.fromElements("es", "ceshi", "test", "shoudao")

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("yb05", 9200, "http"))


    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        //        t: String 传入过来的数据
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val json = new java.util.HashMap[String, String]
          json.put("data", t)
          //          "flinkES02".toLowerCase() es的索引，只能是小写
          val request = Requests.indexRequest().index("flinkESTest".toLowerCase()).`type`("flinkES").source(json)
          requestIndexer.add(request)
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(100)
    //    val value: ElasticsearchSink[String] = esSinkBuilder.build()
    val value= esSinkBuilder.build()
    println(s"a---$value")
    datas.addSink(esSinkBuilder.build())
    env.execute("ES SINK")


  }
}
