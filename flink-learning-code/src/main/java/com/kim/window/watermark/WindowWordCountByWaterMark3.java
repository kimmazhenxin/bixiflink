package com.kim.window.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 *
 * Watermark中的的使用处理
 *  *
 * 依次发送下面的数据:
 * a,10000
 * a,11000
 * a,26000
 * a,12000
 *
 * @Author: kim
 * @Description:
 * @Date: 14:55 2021/5/27
 * @Version: 1.0
 */
public class WindowWordCountByWaterMark3 {

    private  static final Logger logger  = LoggerFactory.getLogger(WindowWordCountByWaterMark3.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //设置waterMark产生的周期为1s
        env.getConfig().setAutoWatermarkInterval(1000);

        // 保存窗口丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};

        if (args.length != 2) {
            logger.error("USAGE:\nSocketWordCount <hostname> <port>");
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        DataStreamSource<String> sourceStream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream = sourceStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] strs = value.split(",");
                if (2 != strs.length) {
                    logger.error("数据格式不对: record = {}", value);
                    return null;
                }
                return Tuple2.of(strs[0], Long.valueOf(strs[1]));
            }
        });

        // 处理
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 指定Watermark
                        .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 提取EventTime
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1)))
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        System.out.println("EventTime = " + value.f1);
                        return Tuple2.of(value.f0, 1L);
                    }})
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(outputTag) // 收集延迟大于2s的数据
                .allowedLateness(Time.seconds(2))
                .process(new WindowWordCountByWaterMark2.SumProcessWindowFunction());

        resultStream.print().setParallelism(1);

        DataStream<Tuple2<String, Long>> sideOutputStream = resultStream.getSideOutput(outputTag);

        // 延迟数据可以保存到其它的介质中
        sideOutputStream.print().setParallelism(1);

        env.execute("WindowWordCountByWaterMark2");
    }


    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型
     * W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        public SumProcessWindowFunction() {
        }

        // 每次窗口触发计算时候,输出窗口中的所有数据
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
            System.out.println("处理时间: " + dateFormat.format(context.currentProcessingTime()));

            System.out.println("当前Watermark: " + context.currentWatermark());

            System.out.println("Window start time: " + context.window().getStart());

            long sum = 0;
            for (Tuple2<String, Long> element : elements) {
                sum += element.f1;
            }

            // 输出当前窗口中的所有数据
            out.collect(Tuple2.of(key, sum));

            System.out.println("Window end time: " + context.window().getEnd());
        }
    }


    // 提取EventTime
    public static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {

        public TimeStampExtractor() {
        }

        //指定时间字段作为EventTime
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            System.out.println("EventTime = " + element.f1);
            return element.f1;
        }
    }
}
