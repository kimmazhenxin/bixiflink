package No37.flink.table.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table/SQL体验
 * @Author: kim
 * @Date: 2020/12/19 13:33
 * @Version: 1.0
 */
public class App {

	public static void main(String[] args) throws Exception {
		// 为了方便测试，flink提供了自动生成数据的source.
		String sourceDDL = "CREATE TABLE random_source (\n" +
				" f_sequence INT,\n" +
				" f_random INT,\n" +
				" f_random_str STRING\n" +
				") WITH (\n" +
				" 'connector' = 'datagen',\n" +
				" 'rows-per-second'='5',\n" +
				" 'fields.f_sequence.kind'='sequence',\n" +
				" 'fields.f_sequence.start'='1',\n" +
				" 'fields.f_sequence.end'='100',\n" +
				" 'fields.f_random.min'='1',\n" +
				" 'fields.f_random.max'='1000',\n" +
				" 'fields.f_random_str.length'='10'\n" +
				")";


		// 为了方便测试，flink提供了控制台打印的print.
		String sinkDDL = "CREATE TABLE print_sink (\n" +
				" f_sequence INT,\n" +
				" f_random INT,\n" +
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

		// 注册source和sink
		tEnv.executeSql(sourceDDL);
		tEnv.executeSql(sinkDDL);

		// 数据摄取
		Table sourceTable = tEnv.from("random_source");
		// 这里我们暂时先使用 标注了 deprecated 的API, 因为新的异步提交测试有待改进...
		sourceTable.insertInto("print_sink");
		// 执行作业
		tEnv.execute("Flink Hello World");
	}


}