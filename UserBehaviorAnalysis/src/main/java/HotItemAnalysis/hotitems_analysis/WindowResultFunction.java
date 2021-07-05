package HotItemAnalysis.hotitems_analysis;

import HotItemAnalysis.hotitems.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于窗口输出的结果
 * @Author: mazhenxin
 * @File: WindowResultFunction.java
 * @Date: 2021/07/03 16:04
 */
public class WindowResultFunction extends ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    public WindowResultFunction() {
    }

    /**
     *
     * @param key       窗口的主键，即 itemId
     * @param context   上下文环境
     * @param elements  聚合函数的结果,即count值
     * @param out       输出的类型为ItemViewCount
     * @throws Exception
     */
    @Override
    public void process(Long key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
        Long count = elements.iterator().next();
        // 输出
        out.collect(new ItemViewCount(key, context.window().getEnd(), count));
    }

//    适用于继承WindowFunction时使用
//    @Override
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
