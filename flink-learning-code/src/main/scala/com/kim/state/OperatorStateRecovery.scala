package com.kim.state

/**
  * Operator State的CheckPoint容错恢复
  * @Author: kim
  * @Date: 2020/9/13 10:14
  * @Version: 1.0
  */
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext, StateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig
import scala.collection.mutable.ListBuffer



//实现CheckpointedFunction接口
class OperatorStateRecoveryRichFunction extends RichFlatMapFunction[String, (Int, String)] with CheckpointedFunction {
	//托管状态
	@transient private var checkPointCountList: ListState[String] = _

	//原始状态
	private var list: ListBuffer[String] = new ListBuffer[String]


	override def flatMap(value: String, out: Collector[(Int, String)]): Unit = {
		if (value.equals("kim")) {
			if (list.size > 0) {
				val outString: String = list.foldLeft("")(_ + " " + _)
				out.collect((list.size, outString))
				list.clear()

			}else if (value.equals("e")) {
				1
			}else {
				list +=value
			}

		}


	}

	//再checkpoint时存储，把正在处理的原始状态的数据保存到托管状态中
	override def snapshotState(context: FunctionSnapshotContext): Unit = {
		checkPointCountList.clear()
		list.foreach(f => checkPointCountList.add(f))
		println(s"snapshotState:${list}")
	}

	//从statebackend中恢复保存的托管状态，并将来数据放到程序处理的原始状态中
	// 出错一次就调用一次这里，能调用几次是根据setRestartStrategy设置的
	override def initializeState(context: FunctionInitializationContext): Unit = {
		val lsd = new ListStateDescriptor[String]("kimListState", TypeInformation.of(new TypeHint[String] {}))
		checkPointCountList = context.getOperatorStateStore.getListState(lsd)
		if (context.isRestored) {
			import scala.collection.convert.wrapAll._
			for (e <- checkPointCountList.get()) {
				list += e
			}
		}
		println(s"initializeState:${list}")

	}
}




object OperatorStateRecovery {
	def main(args: Array[String]): Unit = {
		if (args.length !=2) {
			println("USAGE:\nSocketWordCount <hostname> <port>")
		}
		val hostname: String = args(0)
		val port: Int = args(1).toInt

		//生成配置对象dd
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
		//生产环境使用
		//		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//设置并行度为1,方便测试
		env.setParallelism(1)

		//隔多长时间执行一次ck
		env.enableCheckpointing(1000L)
		val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
		//保存EXACTLY_ONCE
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
		//每次ck之间的间隔，不会重叠
		checkpointConfig.setMinPauseBetweenCheckpoints(500L)
		//每次ck的超时时间,checkpoints have to complete within one minute, or are discarded
		checkpointConfig.setCheckpointTimeout(100L)
		//如果ck执行失败，程序是否停止
		checkpointConfig.setFailOnCheckpointingErrors(true)
		//job在执行CANCE的时候是否删除ck数据
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

		//指定保存ck的存储模式
		val stateBackend = new FsStateBackend("file:///D:/flink/checkpoints", true)
		//        val stateBackend = new MemoryStateBackend(10 * 1024 * 1024,false)
		//    val stateBackend = new RocksDBStateBackend("hdfs://ns1/flink/checkpoints",true)

//		env.setStateBackend(stateBackend)
		env.setStateBackend(stateBackend.asInstanceOf[StateBackend])

		//恢复策略
		env.setRestartStrategy(
			RestartStrategies.fixedDelayRestart(
				3, // number of restart attempts
				Time.of(0, TimeUnit.SECONDS) // delay
			)
		)

		val input: DataStream[String] = env.socketTextStream(hostname, port)
		input.flatMap(new OperatorStateRecoveryRichFunction).print()


		env.execute("operate state checkpoint")
	}

}
