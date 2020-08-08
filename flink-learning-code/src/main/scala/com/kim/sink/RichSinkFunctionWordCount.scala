package com.kim.sink

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  *
  * RichSinkFunction 的使用
  * @Author: kim
  * @Date: 2020/8/8 20:10
  * @Version: 1.0
  */
class RichSinkFunctionWordCount extends RichSinkFunction[(String, Int)]{

	//在Sink开启的时候执行一次，比如可以在这里开启mysql的连接
	//注意:这里是每个并行度（slot）都会调用一次这个open方法,close方法也是一样,所以如果是设置mysql连接池的话一定要调整并行度
	override def open(parameters: Configuration): Unit = {
		println("sink=====>open")
	}

	//在Sink关闭的时候执行一次
	//比如mysql连接用完了，给还回连接池
	//注意:这里是每个并行度（slot）都会调用一次这个open方法,close方法也是一样,所以如果是设置mysql连接池的话一定要调整并行度
	override def close(): Unit = {
		println("sink=====>close")
	}

	//调用invoke方法，执行数据的输出
	override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
		println(s"value:${value}," +
				s"processTime:${context.currentProcessingTime()}," +
				s"waterMark:${context.currentWatermark()}")
	}
}



object RichSinkFunctionWordCount {
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
		result.addSink(new RichSinkFunctionWordCount)
		println("并行度为: " + result.getParallelism)

		env.execute("RichSinkFunctionWordCount")
	}
}