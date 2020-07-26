package com.kim.operators

/**
  * keyBy之后的聚合操作：Reduce、Fold等
  * @Author: kim
  * @Date: 2020/7/26 10:28
  * @Version: 1.0
  */


import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

object ReduceFoldAggregation {
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

		val tuples: List[(String, String, String, Int)] = List(
			("aaaa", "001", "小王", 50),
			("aaaa", "001", "小李", 55),
			("aaaa", "002", "小张", 50),
			("aaaa", "002", "小强", 45))
		// 定义数据源，使用集合生成
		val input: DataStream[(String, String, String, Int)] = env.fromCollection(tuples)

		val map: DataStream[(String, Int)] = input.map(f=> (f._2, 1))
		val keyBy: KeyedStream[(String, Int), String] = map.keyBy(_._1)
		//reduce使用:相同的key的数据聚合在一起使用reduce求合，使用的时候注意与spark不同的地方是key也参与运算
		val reduce: DataStream[(String, Int)] = keyBy.reduce((a, b) => {
			(a._1, a._2 + b._2)
		})
		reduce.print("reduce====>")


		//使用fold完成和reduce一样的功能，不同的是这里的返回值类型由fold的第一个参数决定,每个初始值参与一个Key操作
		val fold: DataStream[(String, Int)] = keyBy.fold(("",5))((a,b) => (b._1,a._2 + b._2))
		fold.print("fold=====>")

		env.execute("ReduceFoldAggregation")
	}

}
