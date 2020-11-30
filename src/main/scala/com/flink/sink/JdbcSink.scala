package com.flink.sink

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink._
import org.apache.flink.streaming.api.scala._

object JdbcSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val datas: DataStream[Employee] = env.fromElements(
      Employee("小明", 18, new Date(System.currentTimeMillis())),
      Employee("小红", 19, new Date(System.currentTimeMillis())),
      Employee("小刚", 32, new Date(System.currentTimeMillis()))
     )
    datas.addSink(new MyJdbc)
    env.execute("JdbcSink")
  }
}

case class Employee(name: String, age: Int, dates: Date)


class MyJdbc extends RichSinkFunction[Employee]{
  var statement: PreparedStatement = _
  var conn: Connection = _
  //打开jdbc的连接
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://yb05:3306/employees?" +
      "characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
      "root",
      "123456"
    )
    //sql操作   插入数据的模板
    statement = conn.prepareStatement("insert into emp(name, age, dates) values(?, ?, ?)")
  }

  //设置对应字段
  override def invoke(value: Employee, context: SinkFunction.Context[_]): Unit = {
    statement.setString(1, value.name)
    statement.setInt(2, value.age)
    statement.setDate(3, value.dates)
    statement.executeUpdate()
  }

  override def close(): Unit = {
    conn.close()
  }
}