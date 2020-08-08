package com.kim.sink

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  * 自定义OutputFormat实现
  *
  * @Author: kim
  * @Date: 2020/8/8 20:36
  * @Version: 1.0
  */
class OutputFormatWordCount extends OutputFormat[(String, Int)]{

	//配置OutputFormat
	override def configure(parameters: Configuration): Unit = {
		println("configure")
	}

	//在Sink开启的时候执行一次，比如可以在这里开启mysql的连接
	//注意:这里是每个并行度（slot）都会调用一次这个open方法,close方法也是一样,所以如果是设置mysql连接池的话一定要调整并行度
	override def open(taskNumber: Int, numTasks: Int): Unit = {
		//taskNumber第几个tak,numTasks总任务数
		println(s"taskNumber:${taskNumber},numTasks:${numTasks}")
		
	}

	//调用writeRecord方法，执行数据的输出
	override def writeRecord(record: (String, Int)): Unit = {
		println(record)
	}

	//在Sink关闭的时候执行一次
	//比如mysql连接用完了，给还回连接池
	//注意:这里是每个并行度（slot）都会调用一次这个open方法,close方法也是一样,所以如果是设置mysql连接池的话一定要调整并行度
	override def close(): Unit = {
		println("sink=====>close")
	}
}


object OutputFormatWordCount {
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

		println("并行度为: " + result.getParallelism)
		//使用Flink自身的OutputFormat实现sink
		result.writeUsingOutputFormat(new OutputFormatWordCount)

		env.execute("OutputFormatWordCount")
	}
}