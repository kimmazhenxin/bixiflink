package com.kim.timeservice;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connect 流实现SessionWindow
 * @Author: mazhenxin
 * @File: ConnectedJoinTimeServiceSessionWindow.java
 * @Date: 2021/1/25 10:32
 */
public class ConnectedJoinTimeServiceSessionWindow {

    private static Logger logger = LoggerFactory.getLogger(ConnectedJoinTimeServiceSessionWindow.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "10.113.31.198";
        DataStreamSource<String> s1 = env.socketTextStream(hostname, 6666);
        DataStreamSource<String> s2 = env.socketTextStream(hostname, 7777);


        KeyedStream<Tuple2<String, Integer>, String> input1 = s1.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(f -> f.f0);
        KeyedStream<Tuple2<String, Integer>, String> input2 = s2.map(f -> Tuple2.of(f, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(f -> f.f0);
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connect = input1.connect(input2);
        SingleOutputStreamOperator<String> process =
                connect.process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    private long interval = 5000;
                    private String outString = "";
                    private long dataTime;
                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        outString += value.f0 + "\t";
                        long currentTimeMillis = System.currentTimeMillis();
                        // 注册3S定时器
                        ctx.timerService().registerProcessingTimeTimer(currentTimeMillis + interval);
                        dataTime = currentTimeMillis;
                        logger.info("subtaskId: " + getRuntimeContext().getIndexOfThisSubtask() + "\tvalue: " + value.f0);
                        logger.info("processElement1 Function currentKey= " + ctx.getCurrentKey() + "\tdataTime= " + dataTime);
                    }
                    @Override
                    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        outString += value.f0 + "\t";
                        long currentTimeMillis = System.currentTimeMillis();
                        // 注册5S定时器
                        ctx.timerService().registerProcessingTimeTimer(currentTimeMillis + interval);
                        dataTime = currentTimeMillis;
                        logger.info("subtaskId: " + getRuntimeContext().getIndexOfThisSubtask() + "\tvalue: " + value.f0);
                        logger.info("processElement2 Function currentKey= " + ctx.getCurrentKey() + "\tdataTime= " + dataTime);
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // timestamp：调用定时器时的时间
                        // 数据最后修改时间加3S等于定时器触发时的时间,那么说明该window的3s内没有数据,即sessionWindow,此时输出数据;否则不输出
                        if (dataTime + interval == timestamp) {
                            out.collect(outString);
                            outString = "";
                            logger.info("match onTimer Function currentKey= " + ctx.getCurrentKey());
                            logger.info("match onTimer Function currentTimeStamp= " + ctx.timestamp());
                        }
                        logger.info("call onTimer...... " + timestamp);
                    }
                }
        );

        process.print();

        env.execute("ConnectedJoinTimeServiceSessionWindow");
    }
}
