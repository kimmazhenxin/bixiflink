package No37.flink.table.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 功能描述: 在SQL程序中设置 启动策略，开启Checkpoint，配置Statebackend等。
 * @Author: kim
 * @Date: 2020/12/19 13:49
 * @Version: 1.0
 */
public class WithExecutionEnvironmentCheckpoint {

	public static void main(String[] args) throws Exception {
		// Kafka {"msg": "welcome flink users..."}
		String sourceDDL = "CREATE TABLE kafka_source (\n" +
				" msg STRING\n" +
				") WITH (\n" +
				" 'connector' = 'kafka-0.11',\n" +
				" 'topic' = 'cdn-log',\n" +
				" 'properties.bootstrap.servers' = 'localhost:9092',\n" +
				" 'format' = 'json',\n" +
				" 'scan.startup.mode' = 'latest-offset'\n" +
				")";

		// Mysql
		String sinkDDL = "CREATE TABLE mysql_sink (\n" +
				" msg STRING \n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"   'url' = 'jdbc:mysql://localhost:3306/flinkdb?characterEncoding=utf-8&useSSL=false',\n" +
				"   'table-name' = 'cdn_log',\n" +
				"   'username' = 'root',\n" +
				"   'password' = '123456',\n" +
				"   'sink.buffer-flush.max-rows' = '1',\n" +
				"   'sink.buffer-flush.interval' = '1s'\n" +
				")";

		// 创建执行环境
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();

		// 配置Checkpoint
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.SECONDS)));
		env.enableCheckpointing(1000L);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		// 注册source和sink
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		// 数据提取
		Table source = tEnv.from("kafka_source");
		// 这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
		source.insertInto("mysql_sink");
		// 执行任务
		tEnv.execute("Flink Table WithExecutionEnvironmentCheckpoint");
	}
}
