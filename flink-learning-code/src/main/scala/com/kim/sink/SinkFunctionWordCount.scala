package com.kim.sink

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  * 自定义SinkFunction
  * @Author: kim
  * @Date: 2020/7/29 23:53
  * @Version: 1.0
  */
class SinkFunctionWordCount extends SinkFunction[(String,Int)] {
	//调用,操作流中的数据
	override def invoke(value: (String,Int), context: SinkFunction.Context[_]): Unit = {
		println(s"value:${value}," +
				s"processTime:${context.currentProcessingTime()}," +
				s"waterMark:${context.currentWatermark()}")
	}
}


object SinkFunctionWordCount {
	def main(args: Array[String]): Unit = {
		//生成配置对象
		val config = new Configuration()
		//开启spark-webui
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
		//配置webui的日志文件，否则打印日志到控制台
		config.setString("web.log.path", "/tmp/flink_log")
		//配置taskManager的日志文件，否则打印日志到控制台
		config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "/tmp/flink_log")
		//配置tm有多少个slot
		config.setString("taskmanager.numberOfTaskSlots", "8")

		// 获取local运行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

		env.setParallelism(1)

		val input: DataStream[String] = env.fromElements("one world one dream we are family")
		val result: DataStream[(String, Int)] = input.flatMap(_.split(" ")).map(x =>(x,1)).keyBy(_._1).sum(1)
		result.print("first===>")
		//使用自定义的sink
		result.addSink(new SinkFunctionWordCount)

		env.execute("SinkFunctionWordCount")


	}

}
