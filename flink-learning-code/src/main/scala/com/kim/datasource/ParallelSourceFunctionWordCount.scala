package com.kim.datasource

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  * 自定义数据源实现ParallelSourceFunction类,并行
  * @Author zhenxin.ma
  * @Date 2020/3/3 19:20
  * @Version 1.0
  */

//ParallelSourceFunction是可以并行化的source
class ParallelSourceFunctionWordCount extends ParallelSourceFunction[String] {
  var num = 0
  var isCancel = true

  //调用run方法向下游产生数据
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isCancel) {
      //相当于把数据往下游发送
      ctx.collect(s"picabi${num}")
      num +=1
    }
  }

  //在cancel的时候被执行，传递变量用于控制run方法中的执行
  //点击WEBUI 界面的 Cancel 按钮时,实际是调用该方法
  override def cancel(): Unit = {
    println("canceling......")
    isCancel = false
  }
}


object ParallelSourceFunctionWordCount {

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
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    // 使用自定义的source
    val text: DataStream[String] = env.addSource(new ParallelSourceFunctionWordCount)

    //定义operator，作用是解析数据，分组，并且求wordcount
    //    val keyStream: KeyedStream[(String, Int), String] = text.flatMap(_.split(" "))
    //      .map((_,1))
    //      .keyBy(_._1)
    //    //累加
    //    val wordCount: DataStream[(String, Int)] = keyStream.sum(1)


    //使用FlatMapFunction自定义函数来完成flatMap和map的组合功能
    //和上面是一样的
    val wordCount: DataStream[(String, Int)] = text.flatMap(new FlatMapFunction[String, (String, Int)] {
      //value:输入	out:输出
      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val strs: Array[String] = value.split(" ")
        for (s <- strs) {
          //把数据输出出去
          out.collect((s, 1))
        }
      }
    }).keyBy(0).sum(1)

    //定义sink打印出控制台
    wordCount.print().setParallelism(2)

    //打印任务的执行计划
    println(env.getExecutionPlan)


    //定义任务的名称并运行
    //注意：operator是惰性的，只有遇到execute才执行
    env.execute("ParallelSourceFunctionWordCount")
  }

}
