package com.kim.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: kim
 * @Date: 2021/4/7 20:52
 * @Version: 1.0
 */
public class BroadcastJoin {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> input1 = env.socketTextStream("192.168.1.9", 6666, '\n');
		DataStreamSource<String> input2 = env.socketTextStream("192.168.1.9", 6666, '\n');
		DataStream<String> broadcast = input1.broadcast();
		input2.connect(broadcast);
		MapStateDescriptor<String, String> mapStateDescriptor =
				new MapStateDescriptor<>("", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		BroadcastStream<String> broadcast1 = input1.broadcast(mapStateDescriptor);

	}
}
