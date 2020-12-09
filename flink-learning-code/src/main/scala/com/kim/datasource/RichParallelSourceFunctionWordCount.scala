package com.kim.datasource

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  *自定义数据源实现RichParallelSourceFunction类,并行
  * @Author zhenxin.ma
  * @Date   2020/3/3 19:23
  * @Version 1.0
  */
//RichParallelSourceFunction不但能并行化
//还比ParallelSourceFunction增加了open和close方法、getRuntimeContext,很有用处这些方法
class RichParallelSourceFunctionWordCount extends RichParallelSourceFunction[String]{
  var num = 0
  var isCancel = true
  //用于判断是否要关闭Mysql的连接
  var isCloseMysql: Boolean = true


  //在source开启的时候执行一次，比如可以在这里开启mysql的连接
  //注意:每个slot都会调用这个方法
  override def open(parameters: Configuration): Unit = {
    //println("source open..........")
    num = 1000
  }


  //在source关闭的时候(run运行完成或者用户主动点webUI上的canceling)执行一次
  //比如mysql连接用完了，给还回连接池
  //注意:每个slot都会调用这个方法
  override def close(): Unit = {
    while (isCloseMysql) {
      //睡眠等待
      Thread.sleep(1000)
    }
    println("close......")
    num = 0
  }

  //调用run方法向下游产生数据
  //手动 cancel 之后，不会等待run方法中处理结束而是强制执行close方法
  //这样就可能导致run方法中正在使用的连接被close了
  //所以此时需要加一个处理完成的标识,用于是否可以进行close
  //注意:每个slot都会调用这个方法
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    import scala.util.control.Breaks._
    breakable {
      while (isCancel) {
        //获取sunTask的索引
        println(getRuntimeContext.getIndexOfThisSubtask)
        ctx.collect(s"picabi${num}")
        Thread.sleep(1000)
        num += 1
        //source的退出条件
        if (num >= 5005) break()
      }
    }
    isCloseMysql = false
  }

  //在cancel的时候被执行，传递变量用于控制run方法中的执行
  //点击WEBUI 界面的 Cancel 按钮时,实际是调用该方法
  override def cancel(): Unit = {
    println("canceling......")
    isCancel = false
  }
}


object RichParallelSourceFunctionWordCount {
  def main(args: Array[String]): Unit = {

    //生成配置对象
    val config: Configuration = new Configuration()
    //开启spark-webui
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true)
    //配置webui的日志文件，否则打印日志到控制台
    config.setString("web.log.path", "/tmp/flink_log")
    //配置taskManager的日志文件，否则打印日志到控制台
    config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"/tmp/flink_log")
    //配置TM有多少个slot
    config.setString("taskmanager.numberOfTaskSlots","8")


    //获取local运行环境并且带上webUI
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 使用自定义的source
    val text: DataStream[String] = env.addSource(new RichParallelSourceFunctionWordCount)

    //定义operator，作用是解析数据，分组，并且求wordcount
    //    val keyStream: KeyedStream[(String, Int), String] = text.flatMap(_.split(" "))
    //      .map((_,1))
    //      .keyBy(_._1)
    //    //累加
    //    val wordCount: DataStream[(String, Int)] = keyStream.sum(1)


    //使用FlatMapFunction自定义函数来完成flatMap和map的组合功能
    //和上面是一样的
//    val wordCount: DataStream[(String, Int)] = text.flatMap(new FlatMapFunction[String, (String, Int)] {
//      //value:输入	out:输出
//      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
//        val strs: Array[String] = value.split(" ")
//        for (s <- strs) {
//          //把数据输出出去
//          out.collect((s, 1))
//        }
//      }
//    }).keyBy(0).sum(1)

    //也可以使用,里面的方法更丰富
    val s: DataStream[(String, Int)] = text.flatMap(new RichFlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val strs: Array[String] = value.split(" ")
        for (s <- strs) {
          out.collect((s, 1))
        }
      }
      override def open(parameters: Configuration): Unit = {
        println("flatMap open ..........................")
        super.open(parameters)
      }
    }).setParallelism(9).keyBy(0).sum(1)


    
    //定义sink打印出控制台
    s.print()

    //打印任务的执行计划
    println(env.getExecutionPlan)

    //定义任务的名称并运行
    //注意：operator是惰性的，只有遇到execute才执行
    env.execute("RichParallelSourceFunctionWordCount")
  }
}