package com.hht.analy.login

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
直接把每次登录失败的数据存起来、设置定时器一段时间后再读取，
这种做法尽管简单，但和我们开始的需求还是略有差异的。
这种做法只能隔2秒之后去判断一下这期间是否有多次失败登录，

而不是在一次登录失败之后、再一次登录失败时就立刻报警。这个需求如果严格实现起来，
相当于要判断任意紧邻的事件，是否符合某种模式

flink为我们提供了CEP（Complex Event Processing，复杂事件处理）库，
用于在流中筛选符合某种复杂模式的事件。接下来我们就基于CEP来完成这个模块的实现
 */
object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.3", "fail", 1558430845),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    // 定义一个匹配模式，next紧邻发生的事件
    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .times(2)
      .within(Time.seconds(2))

    // 在keyby之后的流中匹配出定义好的pattern stream
    val patternStream = CEP.pattern(loginStream, loginFailPattern)

    import scala.collection.Map
    // 从pattern stream 中获取匹配到的事件流
    val loginFailDataStream = patternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
        val begin = pattern.getOrElse("begin", null).iterator.next()
        val next = pattern.getOrElse("next", null).iterator.next()
        (next.userId, begin.ip, next.ip, next.eventType)
      }
    )
      .print()

    env.execute("Login Fail Detect Job")
  }
}
