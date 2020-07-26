package com.kim.operators

/**
  * CoMap、CoFlatMap应用
  * @Author: kim
  * @Date: 2020/7/26 17:35
  * @Version: 1.0
  */

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._

object ConnectCoFlatMap {
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

		val input1: DataStream[Long] = env.generateSequence(1,10)

		val input2: DataStream[String] = env.fromCollection(List("one world one dream"))

		//连接两个流
		val connectInput: ConnectedStreams[Long, String] = input1.connect(input2)

		//flatMap之后的泛型确定了两个流合并之后的返回类型
		val value: DataStream[String] = connectInput.flatMap[String](
			//处理第一个流的数据，需要返回String类型
			(data:Long, out:Collector[String]) => {
				out.collect(data.toString)
			},
			//处理第二个流的数据，需要返回String类型
			(data:String, out:Collector[String]) => {
				val strings: Array[String] = data.split(" ")
				for (s <- strings) {
					out.collect(s)
				}
			}
		)
		//或者传递函数变量
//		val value: DataStream[String] = connectInput.flatMap[String](functiona, functionb)

		//或者使用更加丰富的算子 CoFlatMapFunction、RichCoFlatMapFunction
		//参数1：第一个流元素类型
		//参数2：第二个流元素类型
		//参数3：输出的流元素类型
		val coFlatMapFunction: DataStream[String] = connectInput.flatMap[String](new CoFlatMapFunction[Long, String, String] {

			override def flatMap1(value: Long, out: Collector[String]): Unit = {
				out.collect(value.toString)

			}

			override def flatMap2(value: String, out: Collector[String]): Unit = {
				val strings: Array[String] = value.split(" ")
				for (s <- strings) {
					out.collect(s)
				}
			}
		})
		//这里可以在上述基础上定义变量进行操作,得到变量a
		val coFlatMapFunction1: DataStream[Int] = connectInput.flatMap[Int](new CoFlatMapFunction[Long, String, Int] {
			var a: Int = 1

			override def flatMap1(value: Long, out: Collector[Int]): Unit = {
				a = a + 100
				out.collect(a)

			}
			override def flatMap2(value: String, out: Collector[Int]): Unit = {
				val strings: Array[String] = value.split(" ")
				for (s <- strings) {
					a = a + 2
					out.collect(a)
				}
			}
		})

		coFlatMapFunction1.print("coflatmap===>")

		value.print()
		env.execute()
	}


	private val functiona: (Long, Collector[String]) => Unit = (data: Long, out: Collector[String]) => {
		out.collect(data.toString)
	}

	private val functionb: (String, Collector[String]) => Unit = (data:String, out:Collector[String]) => {
		val strings: Array[String] = data.split(" ")
		for (s <- strings) {
			out.collect(s)
		}
	}
}
