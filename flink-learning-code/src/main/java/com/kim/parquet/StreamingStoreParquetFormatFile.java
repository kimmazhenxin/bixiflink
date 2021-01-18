package com.kim.parquet;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * 流式数据写入parquet文件
 * parquet文件的写入完整性是由checkpoint保证的,即每次checkpoint后才会写入
 * @Author: kim
 * @Date: 2020/12/1 8:16
 * @Version: 1.0
 */
public class StreamingStoreParquetFormatFile {

	private  static final Logger logger  = LoggerFactory.getLogger(StreamingStoreParquetFormatFile.class);

	public static void main(@NotNull String[] args) throws Exception {

		if (args.length != 2) {
			logger.error("USAGE:\nSocketWordCount <hostname> <port>");
		}
		String hostname = args[0];
		int port = Integer.parseInt(args[1]);


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		//隔多长时间执行一次ck
		env.enableCheckpointing(10000L);
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		//保存EXACTLY_ONCE
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		//每次ck之间的间隔，不会重叠
		checkpointConfig.setMinPauseBetweenCheckpoints(500L);
		//每次ck的超时时间,checkpoints have to complete within one minute, or are discarded
		checkpointConfig.setCheckpointTimeout(1000L);
		//如果ck执行失败，程序是否停止
		checkpointConfig.setFailOnCheckpointingErrors(true);
		//job在执行CANCE的时候是否删除ck数据
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		//指定保存ck的存储模式
		//FsStateBackend stateBackend = new FsStateBackend("file:///D:/flink/checkpoints", true);
		MemoryStateBackend memoryStateBackend = new MemoryStateBackend(10 * 1024 * 1024);
		//设置StateBackend
		env.setStateBackend((StateBackend) memoryStateBackend);
		//设定恢复策略
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));


		DataStreamSource<String> streamSource = env.socketTextStream(hostname, port);
		SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource
				.map(m -> Tuple2.of(m, 1L))
				.returns(Types.TUPLE(Types.STRING, Types.LONG))
				.uid("map-tuple")
				.name("map-tuple")
				;

		SingleOutputStreamOperator<Tuple2<String, Long>> sumStream = map
				.keyBy(new ParquetKeySelector())
				.sum(1)
				.uid("sum-stream")
				.name("sum-stream")
				;

		SingleOutputStreamOperator<ParquetPojo> parquetStream = sumStream
				.map(t -> new ParquetPojo(t.f0, t.f1))
				.uid("map-parquet")
				.name("map-parquet")
				;


		// 构建Parquet写入需要的格式
		// 文件的目录形式
		DateTimeBucketAssigner<ParquetPojo> bucketAssigner =
				new DateTimeBucketAssigner<>("yyyy/MM/dd/HH", ZoneId.of("Asia/Shanghai"));
		// 构建要写入的sink
		StreamingFileSink<ParquetPojo> sink = StreamingFileSink
				.forBulkFormat(new Path("/D:/tmp/output/parquet"), ParquetAvroWriters.forReflectRecord(ParquetPojo.class))
				.withBucketAssigner(bucketAssigner)
				.build();

		// 写入文件
		parquetStream.addSink(sink).uid("parquet-stream-sink").name("parquet-stream-sink");


		env.execute("Stream data write parquet format");
	}


	/**
	 * key选择器
	 */
	public static class ParquetKeySelector implements KeySelector<Tuple2<String, Long>, String> {

		@Override
		public String getKey(Tuple2<String, Long> value) throws Exception {
			return value.f0;
		}
	}
}
