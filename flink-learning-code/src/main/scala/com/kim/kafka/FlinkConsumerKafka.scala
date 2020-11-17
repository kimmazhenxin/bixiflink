package com.kim.kafka


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, KafkaDeserializationSchema}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, KeyedDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
//scala开发需要加一行隐式转换，否则在调用operator的时候会报错
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * Flink 消费 kafka
  * @Author: kim
  * @Date: 2020/10/11 10:42
  * @Version: 1.0
  */
object FlinkConsumerKafka {
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
		//val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

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

		//指定保存ck的存储模式,因为kafka的offset比较小,所以kafkaSource推荐使用MemoryStateBackend来保存offset,这样速度快也不会占用过多内存
		val stateBackend: MemoryStateBackend = new MemoryStateBackend(10 * 1024 * 1024, false)
		env.setStateBackend(stateBackend.asInstanceOf[StateBackend])

		//设置恢复策略
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(0, TimeUnit.SECONDS)))

		/*kafka consumer*/
		val kafkaConsumerProps: Properties = new Properties()
		kafkaConsumerProps.setProperty("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
		kafkaConsumerProps.setProperty("group.id", "kim66")
		//当checkpoint启动时这里被自动设置为false
		kafkaConsumerProps.setProperty("enable.auto.commit","false")
		val kafkaSource: FlinkKafkaConsumer010[KafkaEvent] = new FlinkKafkaConsumer010[KafkaEvent]("flink_event", new KafkaEventDeserializationSchema, kafkaConsumerProps)
		//指定timestamp和watermark
		kafkaSource.assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
		//earliest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
		kafkaSource.setStartFromEarliest()
		//latest  当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
		kafkaSource.setStartFromLatest()


		//指定flink的流source为kafkaSource
		val kafkaInput: DataStream[KafkaEvent] = env.addSource(kafkaSource)


	}

}


case class KafkaEvent(message: String, eventTime: Long)





class KafkaEventDeserializationSchema extends KafkaDeserializationSchema[KafkaEvent] {

	override def isEndOfStream(nextElement: KafkaEvent): Boolean = ???

	override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaEvent = ???

	override def getProducedType: TypeInformation[KafkaEvent] = ???
}



class CustomWatermarkExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[KafkaEvent](maxOutOfOrderness) {

	override def extractTimestamp(element: KafkaEvent): Long = {
		1L

	}
}
