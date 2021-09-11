package com.kim.timeservice;


import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Connect 流实现SessionWindow升级版,结合EventTime
 *
 * @Author: mazhenxin
 * @File: ConnectedJoinTimeServiceSessionWindowUpgradeEventTimeMerge.java
 * @Date: 2021/1/29 09:32
 */
public class ConnectedJoinTimeServiceSessionWindowUpgradeEventTimeMerge {

    private static Logger logger = LoggerFactory.getLogger(ConnectedJoinTimeServiceSessionWindowUpgradeEventTimeMerge.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String hostname = "10.113.31.198";
        DataStreamSource<String> s1 = env.socketTextStream(hostname, 6666);
        DataStreamSource<String> s2 = env.socketTextStream(hostname, 7777);

        // ck设置
        env.enableCheckpointing(2000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //指定保存ck的存储模式
        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:/flink/checkpoints", true);
        env.setStateBackend((StateBackend) fsStateBackend);
        //恢复策略
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, // number of restart attempts
                        Time.of(0, TimeUnit.SECONDS) // delay
                )
        );

        // 指定watermark
        KeyedStream<Tuple3<String, Integer, Long>, String> keyedStream1 =
                s1.map(f -> Tuple3.of(f, 1, System.currentTimeMillis()))
                        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<String, Integer, Long> element) {
                                return element.f2;
                            }
                        })
                        .keyBy(f -> f.f0);
        KeyedStream<Tuple3<String, Integer, Long>, String> keyedStream2 =
                s2.map(f -> Tuple3.of(f, 1, System.currentTimeMillis()))
                        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG))
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<String, Integer, Long> element) {
                                return element.f2;
                            }
                        })
                        .keyBy(f -> f.f0);

        ConnectedStreams<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> connect = keyedStream1.connect(keyedStream2);
        SingleOutputStreamOperator<String> process =
                connect.process(new KeyedCoProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String>() {
                    private long interval = 5000;
                    private transient ValueState<Long> dataTime;
                    private transient ReducingState<String> outString;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> dataTimeDesc = new ValueStateDescriptor<>("dataTimeDesc",
                                BasicTypeInfo.LONG_TYPE_INFO);
                        dataTime = getRuntimeContext().getState(dataTimeDesc);
                        ReducingStateDescriptor<String> outStringDesc = new ReducingStateDescriptor<>("outStringDesc"
                                , new RichReduceFunction<String>() {
                            @Override
                            public String reduce(String value1, String value2) throws Exception {
                                return value1 + "\t" + value2;
                            }
                            }, BasicTypeInfo.STRING_TYPE_INFO);
                        outString = getRuntimeContext().getReducingState(outStringDesc);
                    }
                    @Override
                    public void processElement1(Tuple3<String, Integer, Long> value, Context ctx,
                                                Collector<String> out) throws Exception {
                        outString.add(value.f0);
                        long currentTimeMillis = value.f2;
                        // 注册3S定时器
                        ctx.timerService().registerEventTimeTimer(currentTimeMillis + interval);
                        dataTime.update(currentTimeMillis);
                        logger.info("subtaskId: " + getRuntimeContext().getIndexOfThisSubtask() +
                                "\tvalue: " + value.f0);
                        logger.info("processElement1 Function currentKey= " + ctx.getCurrentKey() +
                                "\tdataTime= " + dataTime.value());
                    }
                    @Override
                    public void processElement2(Tuple3<String, Integer, Long> value, Context ctx,
                                                Collector<String> out) throws Exception {
                        outString.add(value.f0);
                        long currentTimeMillis = value.f2;
                        // 注册3S定时器
                        ctx.timerService().registerEventTimeTimer(currentTimeMillis + interval);
                        dataTime.update(currentTimeMillis);
                        logger.info("subtaskId: " + getRuntimeContext().getIndexOfThisSubtask() +
                                "\tvalue: " + value.f0);
                        logger.info("processElement2 Function currentKey= " + ctx.getCurrentKey() +
                                "\tdataTime= " + dataTime.value());
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // timestamp：调用定时器时的时间
                        // 数据最后修改时间加3S等于定时器触发时的时间,那么说明该window的3s内没有数据,即sessionWindow,此时输出数据;否则不输出
                        if (dataTime.value() + interval == timestamp) {
                            out.collect(outString.get());
                            // 清空Key状态
                            outString.clear();
                            logger.info("match onTimer Function currentKey= " + ctx.getCurrentKey());
                            logger.info("match onTimer Function currentTimeStamp= " + ctx.timestamp());
                        }
                        logger.info("call onTimer...... " + timestamp);
                    }
                }
                );

        process.print();

        env.execute("ConnectedJoinTimeServiceSessionWindowUpgrade");
    }


}
