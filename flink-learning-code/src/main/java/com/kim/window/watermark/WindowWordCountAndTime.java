package com.kim.window.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每隔5秒统计最近10秒的元数个数
 * @Author: kim
 * @Description:
 * @Date: 9:06 2021/5/26
 * @Version: 1.0
 */
public class WindowWordCountAndTime {

    private  static final Logger logger  = LoggerFactory.getLogger(WindowWordCountAndTime.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (args.length != 2) {
            logger.error("USAGE:\nSocketWordCount <hostname> <port>");
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        DataStreamSource<String> sourceStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strs = value.split(" ");
                for (String word : strs) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        stream.keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessFunction())
                .print()
                .setParallelism(1);

        env.execute("WindowWordCountAndTime");
    }


    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型
     * W：Window的数据类型
     */
    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("当前系统时间："+dateFormat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间："+dateFormat.format(context.currentProcessingTime()));

            System.out.println("窗口开始时间："+dateFormat.format(context.window().getStart()));
            System.out.println("窗口结束时间："+dateFormat.format(context.window().getEnd()));
            System.out.println("==============================");
            int sum = 0;
            for (Tuple2<String, Integer> element : elements) {
                sum++;
            }
            // 输出
            out.collect(Tuple2.of(key, sum));
        }
    }
}
