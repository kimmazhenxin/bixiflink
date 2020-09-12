package com.kim.operators

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._



/**
  * keyby算子
  * @Author: kim
  * @Date: 2020/7/26 9:37
  * @Version: 1.0
  */

//样例类，可以用于key，因为其默认实现了hashCode方法，可用于对象比较，当然也可用于value
case class StudentEventKey(a:String, b:String, c:String, d:Int)

//普通类可以用于非key，只能用于value
class StudentEventValue(var a:String, var b:String,var c:String, var d:Int) {
	//必须重写toString方法,但是样例类默认是实现的
	override def toString = s"StudentEventValue($a, $b, $c, $d)"
}



object KeyBy {
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

		//对于keyBy算在来说,指定Key的方式有以下几种:
		//第一种:对于元组类型来说数据的选择可以使用数字(从0开始)，keyBy(0,1)这种写法代表组合key
		val keyBy: KeyedStream[(String, String, String, Int), Tuple] = input.keyBy(1)
		//第二种:对于key选择来说还可以使用keySelector,泛型传入输出元素、输入元素,这种方式是更加灵活,根据需求指定key
		val selectorKeyBy: KeyedStream[(String, String, String, Int), String] = input.keyBy(new KeySelector[(String, String, String, Int), String] {
			override def getKey(value: (String, String, String, Int)): String = {
				value._2
			}
		})
		//第三种:根据字段的名字指定key,tuple类型默认的字段名是_1,_2,_3,......
		val _keyBy: KeyedStream[(String, String, String, Int), Tuple] = input.keyBy("_2")


		//对于scala的元组可以使用"_1"、对于java的元组可以使用"f0"，其实也是类中属性的名字
		val maxBy: DataStream[(String, String, String, Int)] = keyBy.maxBy("_4")
//		val maxBy: DataStream[(String, String, String, Int)] = keyBy.maxBy(3)
		maxBy.print()


		//通过样例类试验构造数据源
		val students: List[StudentEventKey] = List(
			StudentEventKey("aaaa", "001", "小王", 50),
			StudentEventKey("aaaa", "001", "小李", 55),
			StudentEventKey("aaaa", "002", "小张", 50),
			StudentEventKey("aaaa", "002", "小强", 45))
		val studentInput: DataStream[StudentEventKey] = env.fromCollection(students)
		//第四种:对于自定义类型来说也可以用类中的字段名称，记住这个自定义类型必须得是样例类。
		val studentKeyBy: KeyedStream[StudentEventKey, Tuple] = studentInput.keyBy("b")
		val studentMaxBy: DataStream[StudentEventKey] = studentKeyBy.maxBy("d")
		studentMaxBy.print()
		studentKeyBy.map(f => new StudentEventValue(f.a, f.b, f.c, f.d)).print()


		env.execute("key by")
	}



}
