package com.kim.datasource

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * 本地开发,读取socket统计wordcount
 *
	* @Author zhenxin.ma
  * @Date 	2020/3/2 15:13
  * @Version 1.0
  */
object SocketWordCount {
	def main(args: Array[String]): Unit = {
		if (args.length !=2) {
			println("USAGE:\nSocketWordCount <hostname> <port>")
		}
		val hostname: String = args(0)
		val port: Int = args(1).toInt

		//生成配置对象
		val config: Configuration = new Configuration()
		//开启flink-webui
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true)
		//配置webui的日志文件，否则打印日志到控制台
		config.setString("web.log.path", "/tmp/flink_log")
		//配置taskManager的日志文件，否则打印日志到控制台
		config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"/tmp/flink_log")
		//配置TM有多少个slot
		config.setString("taskmanager.numberOfTaskSlots","8")


		//获取local运行环境并且带上webUI
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
//		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))
		//定义socket源
		val text: DataStream[String] = env.socketTextStream(hostname,port)


		//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
		import org.apache.flink.api.scala._

		//定义operator，作用是解析数据，分组，并且求wordcount
		val keyStream: KeyedStream[(String, Int), String] = text.flatMap(_.split(" "))
			.map((_,1))
			.keyBy(_._1)
		//累加
		val wordCount: DataStream[(String, Int)] = keyStream.sum(1)


		//使用FlatMapFunction自定义函数来完成flatMap和map的组合功能
		//和上面是一样的
		val wordCountTo: DataStream[(String, Int)] = text.flatMap(new FlatMapFunction[String, (String, Int)] {
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
		wordCount.print()

		//打印任务的执行计划
		println(env.getExecutionPlan)

		//定义任务的名称并运行
		//注意：operator是惰性的，只有遇到execute才执行
		env.execute("SocketWordCount")
  }
}
