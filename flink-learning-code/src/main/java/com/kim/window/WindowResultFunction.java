package com.kim.window;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于窗口输出的结果
 * @Author: mazhenxin
 * @File: WindowResultFunction.java
 * @Date: 2020/11/30 19:52
 */
public class WindowResultFunction extends ProcessWindowFunction<Long, ItemViewCount , Long, TimeWindow> {

    public WindowResultFunction() {
    }

    @Override
    public void process(
            Long key,     // 窗口的主键，即 itemId
            Context context,     // 窗口
            Iterable<Long> elements,
            Collector<ItemViewCount> out) throws Exception {
        Long count = elements.iterator().next();
        out.collect(new ItemViewCount(key, context.window().getEnd(), count));


    }

//    适用于WindowFunction
//    public void apply(
//            Long key,    // 窗口的主键，即 itemId
//            TimeWindow window,  // 窗口
//            Iterable<Long> input,   // 聚合函数的结果,即count值
//            Collector<ItemViewCount> out    //输出的类型为ItemViewCount
//    ) throws Exception {
//        Long count = input.iterator().next();
//        out.collect(new ItemViewCount(key, window.getEnd(), count));
//    }

}
