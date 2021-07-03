package com.kim.table.retract;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: kim
 * @Description: Retract流
 * @Date: 2021/6/19 10:29
 * @Version: 1.0
 */
public class RetractDemo {

	public static void main(String[] args) throws Exception {
		// Flink 环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 指定使用BlinkPlanner
		EnvironmentSettings envSetting = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();

		// 创建Flink SQL运行时环境
		StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env, envSetting);

		DataStreamSource<WebVisit> dataStreamSource = env.addSource(new WebVisitSource());

		// 从DataStream中创建一个Table，并指定了Table的属性
		Table tblWebVisit  = tblEnv.fromDataStream(dataStreamSource, $("ip"), $("cookieId"), $("pageUrl"), $("openTime"), $("browser"));

		// 实现聚合操作
		Table table = tblWebVisit.groupBy($("browser")).select($("browser"), $("pageUrl").count().as("cnt"));

		// 此处将表转换为RetractStream,因为之前的聚合操作显然是存在不断的UPDATE的.
		DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tblEnv.toRetractStream(table, Row.class);
		tuple2DataStream.print();


		env.execute("Flink SQL");
	}





}
