package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.PageViewCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 求某个窗口中前 N 名的热门点击网页，key 为窗口结束的时间戳，输出为 TopN 的结果字符串
 * @Author: kim
 * @Description:
 * @Date: 17:50 2021/7/6
 * @Version: 1.0
 */
public class TopNHotPages extends KeyedProcessFunction<String, PageViewCount, String> {

    private static final Logger logger = LoggerFactory.getLogger(TopNHotPages.class);

    private final int topSize;




    public TopNHotPages(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);


    }


    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {

    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }
}
