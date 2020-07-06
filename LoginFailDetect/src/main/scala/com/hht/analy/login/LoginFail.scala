package com.hht.analy.login

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//在这个子模块中，我们将会用到flink的CEP库来实现事件流的模式匹配，
// 所以需要在pom文件中引入CEP的相关依赖
object LoginFail {
  /**
    * 对于网站而言，用户登录并不是频繁的业务操作。
    * 如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。
    * 因此我们考虑，应该对用户的登录失败动作进行统计，
    * 具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
    * 输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
    */

  /**
    * 最简单的方法其实与之前的热门统计类似，只需要按照用户ID分流，
    * 然后遇到登录失败的事件时将其保存在ListState中，然后设置一个定时器，2秒后触发。
    * 定时器触发时检查状态中的登录失败事件个数，如果大于等于2，那么就输出报警信息
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val value: DataStreamSink[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()

    env.execute(" login fail job")
  }
}
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

class MatchFunction() extends KeyedProcessFunction[Long,LoginEvent,LoginEvent]{

  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login",classOf[LoginEvent]))

  override def processElement(loginEvent: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = {
    //失败了加进去
    if(loginEvent.eventType == "fail"){
      loginState.add(loginEvent)
    }
    //设置一个定时器，事件时间2秒后触发
    context.timerService().registerEventTimeTimer(loginEvent.eventTime + 2 * 1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    val allLoginEvents: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for( urlView <- loginState.get() ){
      allLoginEvents += urlView
    }
    loginState.clear()

    if(allLoginEvents.size > 1){
      //可以自定义报警信息，我们这边直接把登录的信息返回
      out.collect(allLoginEvents.head)
    }

  }

}