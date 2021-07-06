package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: kim
 * @Description:
 * @Date: 17:42 2021/7/6
 * @Version: 1.0
 */
public class PageCountWindowResult extends ProcessWindowFunction<Long, PageViewCount, String, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(PageCountWindowResult.class);


    public PageCountWindowResult() {
    }

    @Override
    public void process(String s, Context context, Iterable<Long> elements, Collector<PageViewCount> out) throws Exception {

    }
}
