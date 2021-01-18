package No22.out.of.order;

import No31.e2e.exactlyonce.E2eExactlyOnceTestCase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * watermark使用
 * @Author: mazhenxin
 * @File: OutOfOrderCase.java
 * @Date: 2020/12/8 8:48
 */
public class OutOfOrderCase {

    private  static  final Logger logger = LoggerFactory.getLogger(E2eExactlyOnceTestCase.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                ctx.collect(new Tuple2<>("key", 0L));
                ctx.collect(new Tuple2<>("key", 1000L));
                ctx.collect(new Tuple2<>("key", 2000L));
                ctx.collect(new Tuple2<>("key", 3000L));
                ctx.collect(new Tuple2<>("key", 4000L));
                ctx.collect(new Tuple2<>("key", 5000L));

                // out of order
                ctx.collect(new Tuple2<>("key", 4000L));
                ctx.collect(new Tuple2<>("key", 6000L));
                ctx.collect(new Tuple2<>("key", 6000L));
                ctx.collect(new Tuple2<>("key", 7000L));
                ctx.collect(new Tuple2<>("key", 8000L));
                ctx.collect(new Tuple2<>("key", 10000L));

                // out of order
                ctx.collect(new Tuple2<>("key", 8000L));
                ctx.collect(new Tuple2<>("key", 9000L));

                //ctx.collect(new Tuple2<>("key", 4000L));
                //ctx.collect(new Tuple2<>("key", 4000L));
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> single = source.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, Long>>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Tuple2<String, Long> lastElement, long extractedTimestamp) {
                return null;
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return 0;
            }
        });

    }
}
