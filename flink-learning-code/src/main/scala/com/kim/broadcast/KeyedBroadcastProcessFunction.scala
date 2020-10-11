package com.kim.broadcast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.log4j.Logger
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  *
  * @Author: kim
  * @Date: 2020/9/26 17:57
  * @Version: 1.0
  */





object KeyedBroadcastProcessFunction {
	val LOG = Logger.getLogger(this.getClass)

	//broadcast的类型描述，也可以在broadCastProcessFunction中重复使用
	val configBroadCastState =
		new MapStateDescriptor[String, CountryConfig]("configBroadCastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[CountryConfig]() {}))

	def main(args: Array[String]): Unit = {

		//生成配置对象dd
		val config: Configuration = new Configuration()
		//开启flink-webui
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true)
		//配置webui的日志文件，否则打印日志到控制台
		config.setString("web.log.path", "/tmp/flink/flink_log")
		//配置taskManager的日志文件，否则打印日志到控制台
		config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"/tmp/fink/flink_log")
		//配置TM有多少个slot
		config.setString("taskmanager.numberOfTaskSlots","8")


		//获取local运行环境并且带上webUI
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
		//生产环境使用
		//		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

		//第一步: 创建常规事件流DataStream / KeyedDataStream, 定义socket源
		val input1: DataStream[String] = env.socketTextStream("192.168.1.9", 6666, '\n')
		val input2: DataStream[String] = env.socketTextStream("192.168.1.9", 7777, '\n')

		//第二步: 创建BroadcastedStream, 将input2变成广播流，广播到所有task中
		val broadcast: BroadcastStream[CountryConfig] = input2.map(r => {
			val strings: Array[String] = r.split(" ")
			CountryConfig(strings(0), strings(1))
		}).broadcast(configBroadCastState)

		//第三步: 连接两个流,生成BroadcastConnectedStream并实现计算处理
		val broadcastConnect: BroadcastConnectedStream[String, CountryConfig] = input1.connect(broadcast)
		broadcastConnect.process(new BixiBroadcastProcessFunction).print()



		env.execute("Broadcast State")
	}

}
