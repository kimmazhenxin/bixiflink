package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于窗口输出的结果,关键是拿到窗口的元数据信息
 * @Author: kim
 * @Description:
 * @Date: 17:42 2021/7/6
 * @Version: 1.0
 */
public class PageCountWindowResult extends ProcessWindowFunction<Long, PageViewCount, String, TimeWindow> {
    // IN, 这里是聚合后输入的类型
    // OUT, 输出类型
    // KEY, Key类型
    // W extends Window, 窗口的类型

    private static final Logger logger = LoggerFactory.getLogger(PageCountWindowResult.class);


    public PageCountWindowResult() {
    }

    @Override
    public void process(String url, Context context, Iterable<Long> elements, Collector<PageViewCount> out) throws Exception {
        // 窗口结束时间戳
        long windowEnd = context.window().getEnd();
        // 每个key窗口聚合后拿到的统计结果
        Long viewCount = elements.iterator().next();
        // 输出
        out.collect(new PageViewCount(url, windowEnd, viewCount));
    }
}
