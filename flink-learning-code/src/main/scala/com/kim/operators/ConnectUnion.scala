package com.kim.operators

/**
  * connect、union算子
  * @Author: kim
  * @Date: 2020/7/26 11:11
  * @Version: 1.0
  */


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._


object ConnectUnion {
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
		config.setString("taskmanager.numberOfTaskSlots", "12")

		// 获取local运行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

		val input1: DataStream[Long] = env.generateSequence(0, 10)

		val input2: DataStream[String] = env.fromCollection(List("one world one dream"))

		//连接两个流
		val connectInput: ConnectedStreams[Long, String] = input1.connect(input2)

		//使用connect连接两个流，类型可以不一致,不同的操作分别应用于不同的流
		//Map之后的泛型确定了两个流合并之后的返回类型
		val connect: DataStream[String] = connectInput.map[String](
			//处理第一个流的数据，需要返回String类型
			(a: Long) => a.toString,
			//处理第二个流的数据，需要返回String类型
			(b: String) => b.concat(" common on"))
		//或者下面这种方式,传递函数变量
//		connectInput.map(functiona, functionb)

		//输出
		connect.print("connectStream ==>")


		//union操作
		val input3: DataStream[Long] = env.generateSequence(11, 20)
		val input4: DataStream[Long] = env.generateSequence(21, 30)
		//使用union连接多个流，要求数据类型必须一致，且返回结果是DataStream
		input1.union(input3).union(input4).print()

		env.execute()
	}


	//定义两个函数变量
	private val functiona: Long => String = (a:Long) => a.toString
	private val functionb: String => String = (b:String) => b.concat(" common on")

}
