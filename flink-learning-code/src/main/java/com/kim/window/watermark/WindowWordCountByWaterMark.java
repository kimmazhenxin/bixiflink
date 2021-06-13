package com.kim.window.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;



/**
 *
 * 需求：3秒一个窗口,把相同 key合并起来,依次输入下面事件:
 * hadoop,1461756862000
 * hadoop,1461756866000
 * hadoop,1461756872000
 * hadoop,1461756873000
 * hadoop,1461756874000
 * hadoop,1461756876000
 * hadoop,1461756877000
 * hadoop,1461756878000
 * hadoop,1461756873000
 * hadoop,1461756888000
 * window + watermark  观察窗口是如何被触发？
 *
 * 可以解决乱序问题
 * @Author: kim
 * @Description:
 * @Date: 17:23 2021/5/26
 * @Version: 1.0
 */
public class WindowWordCountByWaterMark {

    private  static final Logger logger  = LoggerFactory.getLogger(WindowWordCountByWaterMark.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //设置waterMark产生的周期为1s
        env.getConfig().setAutoWatermarkInterval(1000);

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

        // 获取EventTime
        mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 指定Watermark
                        .forGenerator(record -> new PeriodicWatermarkGenerator()) // 方式一
                        //.forGenerator(new PeriodicWatermark()) // 方式二
                        // 指定时间字段
                        .withTimestampAssigner(record -> new TimeStampExtractor()))
                .keyBy(tuple -> tuple.f0)
                //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(2)))
                //.allowedLateness(Time.seconds(2)) // 允许延迟两秒处理
                .process(new SumProcessWindowFunction())
                .print()
                .setParallelism(1);


        env.execute("WindowWordCountByWaterMark");
    }

    // Watermark产生
    public static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        // 当前窗口里面的最大的事件时间
        private long currentMaxEventTime = 0L;
        // 最大允许的乱序时间 10 秒
        private long maxOutOfOrderness = 10000;

        public PeriodicWatermarkGenerator() {
        }

        // 每条元素都调用这个方法,可以每条都产生Watermark,也可以根据实际情况自定义某种条件产生
        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
            long currentElementEventTime = event.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

            // 打印当前处理线程的id,查看每个线程的Watermark
            long id = Thread.currentThread().getId();

            System.out.println("当前处理线程ID = " + id + "| event = " + event
                    + "| " + dateFormat.format(event.f1) // Event Time
                    + "| " + dateFormat.format(currentMaxEventTime) // Max Event Time
                    + "| " + dateFormat.format(currentMaxEventTime - maxOutOfOrderness)); // Current Watermark


        }

        // 周期性地产生Watermark,默认是200ms,可以设置
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 计算出来Watermark的值, Watermark必须是单调递增的
            output.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
        }
    }

    // Watermark产生,和上面逻辑一样
    public static class PeriodicWatermark implements WatermarkGeneratorSupplier<Tuple2<String, Long>> {

        // 实现该接口的方法
        @Override
        public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(Context context) {

            return new WatermarkGenerator<Tuple2<String, Long>>() {

                FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

                // 当前窗口里面的最大的事件时间
                private long currentMaxEventTime = 0L;
                // 最大允许的乱序时间 10 秒
                private long maxOutOfOrderness = 10000;

                // 每条元素都调用这个方法,可以每条都产生Watermark,也可以根据实际情况自定义某种条件产生
                @Override
                public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                    long currentElementEventTime = event.f1;
                    currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);


                    System.out.println("|event = " + event
                            + "|" + dateFormat.format(event.f1) // Event Time
                            + "|" + dateFormat.format(currentMaxEventTime) // Max Event Time
                            + "|" + dateFormat.format(currentMaxEventTime - maxOutOfOrderness)); // Current Watermark


                }

                // 周期性地产生Watermark,默认是200ms,可以设置
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // 计算出来Watermark的值, Watermark必须是单调递增的
                    output.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
                }
            };
        }
    }


    // 提取EventTime
    public static class TimeStampExtractor implements TimestampAssigner<Tuple2<String, Long>> {

        public TimeStampExtractor() {
        }

        //指定时间字段作为EventTime
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型
     * W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        public SumProcessWindowFunction() {
        }

        // 每次窗口触发计算时候,输出窗口中的所有数据
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            System.out.println("处理时间: " + dateFormat.format(context.currentProcessingTime()));

            System.out.println("当前Watermark: " + dateFormat.format(context.currentWatermark()));

            System.out.println("Window start time: " + dateFormat.format(context.window().getStart()));

            List<String> data = new ArrayList<>();
            for (Tuple2<String, Long> element : elements) {
                data.add(element.toString() + "|" + dateFormat.format(element.f1));
            }
            // 输出当前窗口中的所有数据
            out.collect(data.toString());

            System.out.println("Window end time: " + dateFormat.format(context.window().getEnd()));
        }
    }
}
