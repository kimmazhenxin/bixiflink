package com.kim.window.join;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * IntervalJoin的使用案例
 * @Author: kim
 * @Description:
 * @Date: 19:01 2021/5/28
 * @Version: 1.0
 */
public class IntervalJoinUse {

    private  static final Logger logger  = LoggerFactory.getLogger(IntervalJoinUse.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (args.length != 3) {
            logger.error("USAGE:\nSocketWordCount <hostname> <port>");
        }
        String hostname = args[0];
        int port1 = Integer.parseInt(args[1]);
        int port2 = Integer.parseInt(args[2]);


        DataStreamSource<String> sourceStream1 = env.socketTextStream(hostname, port1);
        DataStreamSource<String> sourceStream2 = env.socketTextStream(hostname, port2);
        SingleOutputStreamOperator<Tuple3<String, Long, String>> stream1 = sourceStream1.flatMap(new FlatMapFunction<String, Tuple3<String, Long, String>>() {

            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Long, String>> out) throws Exception {
                String[] words = value.split(",");
                System.out.println("stream1 ==>words[1] = " + dateFormat.format(Long.valueOf(words[1])));
                out.collect(Tuple3.of(words[0], Long.valueOf(words[1]), words[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1)));

        SingleOutputStreamOperator<Tuple3<String, Long, String>> stream2 = sourceStream2.flatMap(new FlatMapFunction<String, Tuple3<String, Long, String>>() {

            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Long, String>> out) throws Exception {
                String[] words = value.split(",");
                System.out.println("stream2 ==> words[1] = " + dateFormat.format(Long.valueOf(words[1])));
                out.collect(Tuple3.of(words[0], Long.valueOf(words[1]), words[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1)));


        // intervalJoin
        stream1.keyBy(tuple -> tuple.f0)
                .intervalJoin(stream2.keyBy(tuple -> tuple.f0))
                .between(Time.seconds(-1), Time.seconds(1))
                .process(new MyProcessJoinFunction())
                .print()
                .setParallelism(1);


        env.execute("IntervalJoinUse");
    }


    // join处理
    public static class MyProcessJoinFunction extends ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, Tuple2<String, String>> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        public MyProcessJoinFunction() {
        }


        @Override
        public void processElement(Tuple3<String, Long, String> left, Tuple3<String, Long, String> right, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {

            long leftTimestamp = ctx.getLeftTimestamp();
            long rightTimestamp = ctx.getRightTimestamp();
            long timestamp = ctx.getTimestamp();

            System.out.println("leftTimestamp = " + dateFormat.format(leftTimestamp));
            System.out.println("timestamp = " + dateFormat.format(timestamp));
            System.out.println("rightTimestamp = " + dateFormat.format(rightTimestamp));

            System.out.println("=========================");
            System.out.println("left  : " + left);
            System.out.println("right : " + right);


            out.collect(Tuple2.of(left.f2, right.f2));
        }
    }


}
