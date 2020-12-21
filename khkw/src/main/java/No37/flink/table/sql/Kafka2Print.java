package No37.flink.table.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能描述: 从Kafka读取消息数据，并在控制台进行打印。
 * @Author: kim
 * @Date: 2020/12/19 13:47
 * @Version: 1.0
 */
public class Kafka2Print {

	public static void main(String[] args) throws Exception {
		// Kafka
		String sourceDDL = "CREATE TABLE kafka_source (\n" +
				" log_msg STRING\n" +
				") WITH (\n" +
				" 'connector' = 'kafka-0.11',\n" +
				" 'topic' = 'cdn-log',\n" +
				" 'properties.bootstrap.servers' = 'localhost:9092',\n" +
				" 'format' = 'json',\n" +
				" 'scan.startup.mode' = 'latest-offset'\n" +
				")";

		// Mysql
		String sinkDDL = "CREATE TABLE print_sink (\n" +
				" f_random_str STRING \n" +
				") WITH (\n" +
				" 'connector' = 'print'\n" +
				")";

		// 创建执行环境
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
		TableEnvironment tEnv = TableEnvironment.create(settings);

		//注册source和sink
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		//数据提取
		Table sourceTab = tEnv.from("kafka_source");
		//这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
		sourceTab.insertInto("print_sink");
		//执行作业
		tEnv.execute("Flink Table Kafka2Print");
	}
}
