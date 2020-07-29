package com.kim.operators


import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

/**
  * 自定义partitioner
  * @Author: kim
  * @Date: 2020/7/29 23:14
  * @Version: 1.0
  */
object CustomPartitioner {
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

		val tuples: List[(String, String, String, Int)] = List(
			("aaaa", "001", "小王", 50),
			("aaaa", "001", "小李", 55),
			("aaaa", "002", "小张", 50),
			("aaaa", "002", "小强", 45))
		// 定义数据源，使用集合生成
		val input: DataStream[(String, String, String, Int)] = env.fromCollection(tuples)

		//第二个参数 _._2 是指定partitioner的key是数据中的那个字段
		input.partitionCustom(new CustomPartitioner,_._2).print()


		env.execute("custom partitioner")
	}


}

// 自定义partiitoner,传入的泛型代表的是 key 的类型
class CustomPartitioner extends Partitioner[String] {
	//key满足条件,那么数据进入到索引为3的slot,否则进入其它
	//返回的是 The partition index
	override def partition(key: String, numPartitions: Int): Int = {
		if (key.equals("001")) {
			return 3
		}else {
			return numPartitions -1
		}
	}
}
