package com.hht.flink.analy

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门商品统计
  * 抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
  * 过滤出点击行为数据
  * 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
  * 按每个窗口聚合，输出每个窗口中点击量前N名的商品
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\Desktop\\flink-analy\\src\\main\\resources\\UserBehavior.csv")

    val topNDStream: DataStream[String] = dStream.map(line => {
      val lineArr: Array[String] = line.split(",")
      UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
    })
      // 指定时间戳和watermark
      //真实业务场景一般都是乱序的，所以一般不用assignAscendingTimestamps，
      //         而是使用BoundedOutOfOrdernessTimestampExtractor。
      .assignAscendingTimestamps(_.timestamp * 1000)
      //过滤出点击事件
      .filter(_.behavior == "pv")
      // keyBy("itemId")对商品进行分组
      //flink用_.itmeId会出问题，这个习惯和spark不太一样
      //   可以感觉到flink的也是偏向sql化的
      .keyBy("itemId")
      //使用.timeWindow(Time size, Time slide)对每个商品做滑动窗口（1小时窗口，5分钟滑动一次）
      .timeWindow(Time.hours(1), Time.minutes(5))
      //.aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作
      //   它能使用AggregateFunction提前聚合掉数据，减少state的存储压力
      //   较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，最后一起计算要高效地多
      //   CountAgg实现了AggregateFunction接口，功能是统计窗口中的条数，即遇到一条数据就加一,类似累加器
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      //ProcessFunction是Flink提供的一个low-level API，用于实现更高级的功能。
      // 它主要提供了定时器timer的功能（支持EventTime或ProcessingTime）
      .process(new TopNHotItems(3))

    val value: DataStreamSink[String] = topNDStream.print()

    env.execute("hot items job")

  }

}

//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//输出数据样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

// COUNT统计的自定义聚合函数实现，每出现一条记录就加一
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


//第二个参数WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出。
// 我们这里实现的WindowResultFunction将<主键商品ID，窗口，点击量>封装成了ItemViewCount进行输出。
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  //WindowFunction[IN, OUT, KEY, W <: org.apache.flink.streaming.api.windowing.windows.Window]
  // 这个IN就是CountAgg的输出，
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //这个key就是item，取出来
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}

//KeyedProcessFunction<K, I, O>
//   参数key为窗口时间戳 input  output为 TopN 的结果字符串
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //定义状态ListState，scala给初值的比较好的办法_而不是null
  //使用了ListState<ItemViewCount>来存储收到的每条ItemViewCount消息，
  //      保证在发生故障时，状态数据的不丢失和一致性。
  // ListState是Flink提供的类似Java List接口的State API，
  //     它集成了框架的checkpoint机制，自动做到了exactly-once的语义保证。
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //命名状态变量的名字和类型
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }
  //由于Watermark的进度是全局的，在processElement方法中，
  //             每当收到一条数据ItemViewCount都要itemState.add(itemViewCount)，
  //             我们就注册一个windowEnd+1的定时器（Flink框架会自动忽略同一时间的重复注册）。
  // windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的Watermark，
  //             即收齐了该windowEnd下的所有商品窗口统计值
  override def processElement(itemViewCount: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(itemViewCount)
    //注册定时器，触发时间定为windpowEnd + 1,触发说明window已经收集完成所有的数据
    context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 1)
  }

  //定时器触发操作，从state里面取出所有数据，排序取出TOPN，输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //ListState操作不方便，还是scala的集合用起来方便
    val allItems:ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get){
      allItems += item
    }

    //清楚状态中的数据，释放空间
    itemState.clear()

    //按照点击量从大到小排序,选取TOPN
    // sortBy是从小到大，用函数科里化sortBy()(Ordering.Long.reverse)
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse)


    // 将排名数据格式化，然后打印输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for( i <- sortedItems.indices ){
      val currentItem: ItemViewCount = sortedItems(i)
      // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i+1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率
    Thread.sleep(100)

    out.collect(result.toString)
  }
}