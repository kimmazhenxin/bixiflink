package com.kim.window.trigger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * 使⽤用Evictor ⾃自⼰己实现⼀一个类似CountWindow(3,2)的效果
 * 每隔2个单词计算最近3个单词
 * @Author: kim
 * @Description:
 * @Date: 16:49 2021/5/28
 * @Version: 1.0
 */
public class CountWindowWordCountByEvictor {

    private  static final Logger logger  = LoggerFactory.getLogger(CountWindowWordCountByEvictor.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        if (args.length != 2) {
            logger.error("USAGE:\nSocketWordCount <hostname> <port>");
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);


        DataStreamSource<String> sourceStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = sourceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        stream.keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create())
                .evictor(new MyEvictor(2))
                .trigger(new MyCountTrigger(3))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print()
                .setParallelism(1);


        env.execute("CountWindowWordCount");
    }


    /**
     * 自定义实现countTrigger
     */
    public static class MyCountTrigger extends Trigger<Tuple2<String, Long>, GlobalWindow> {

        // 表示指定元素的最大数量,就是类似于CountWindow中的size值
        private long maxCount;

        // 用于存储每个key对应的count值
        private ReducingStateDescriptor<Long> reducingStateDescriptor = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>(){

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }, Long.class);



        public MyCountTrigger() {
        }

        public MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        // 每个元素被添加到窗口时调用
        /**
         * 当一个元素进⼊到⼀个 window 中的时候就会调用这个方法
         * @param element 元素
         * @param timestamp 进来的时间
         * @param window 元素所属的窗口
         * @param ctx 上下文
         * @return TriggerResult
         * 1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         * 2. TriggerResult.FIRE ：表示触发 window 的计算
         * 3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         * 4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除
        window 中的数据
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 拿到当前 key 对应的 count 状态值
            ReducingState<Long> reducingState = ctx.getPartitionedState(reducingStateDescriptor);
            // count 累加 1
            reducingState.add(1L);
            // 如果当前 key 的 count 值等于 maxCount
            if (reducingState.get() == maxCount) {
                // 清空存储的状态值
                reducingState.clear();
                // 触发 window 计算，删除数据
                return TriggerResult.FIRE_AND_PURGE;
            }
            // 否则,对 window 不不做任何的处理理
            return TriggerResult.CONTINUE;
        }

        // 当一个已注册的处理时间计时器启动时调用
        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 写基于 Processing Time 的定时器器任务逻辑
            return TriggerResult.CONTINUE;
        }

        // 当一个已注册的事件时间计时器启动时调用
        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 写基于 Event Time 的定时器器任务逻辑
            return TriggerResult.CONTINUE;
        }

        /**
         * 相应窗口被清除时触发该方法
         */
        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(reducingStateDescriptor).clear();

        }

        // 与状态性触发器相关，当使用session window时，两个触发器对应的窗口合并时，合并两个触发器的状态。
        @Override
        public void onMerge(GlobalWindow window, OnMergeContext ctx) throws Exception {
            super.onMerge(window, ctx);
        }
    }


    /**
     * 自定义实现Evictor
     */
    public static class MyEvictor implements Evictor<Tuple2<String, Long>, GlobalWindow> {

        // 窗口的大小
        private long winCount;

        private boolean doEvictBefore;

        public MyEvictor() {
        }

        public MyEvictor(long winCount) {
            this.winCount = winCount;
        }

        /**
         * 在 window 计算之前删除特定的数据
         * @param elements window 中所有的元素
         * @param size window 中所有元素的大小
         * @param window window
         * @param evictorContext 上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (doEvictBefore) {
                evict(elements, size, evictorContext);
            }

        }


        /**
         * 在 window 计算之后删除特定的数据
         * @param elements window 中所有的元素
         * @param size window 中所有元素的大小
         * @param window window
         * @param evictorContext 上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if(!doEvictBefore) {
                evict(elements, size, evictorContext);
            }

        }


        // 执行逻辑
        public void evict(Iterable<TimestampedValue<Tuple2<String, Long>>> elements, int size, EvictorContext evictorContext) {
            System.out.println("size = " + size);
            // 窗口元素数量小于窗口大小
            if (size < winCount) {
                return;
            } else {
                int evictorCount = 0;
                for (Iterator<TimestampedValue<Tuple2<String, Long>>> iterator = elements.iterator(); iterator.hasNext();) {
                    TimestampedValue<Tuple2<String, Long>> next = iterator.next();
                    Tuple2<String, Long> value = next.getValue();
                    System.out.println("element = " + value.toString());
                    evictorCount++;
                    // 如果删除的数量⼩于当前的 window 大小减去规定的 window 的大小，就需要删除当前的元素
                    if (evictorCount > size - winCount) {
                        break;
                    } else {
                        iterator.remove();
                    }
                }
            }
        }

    }
}
