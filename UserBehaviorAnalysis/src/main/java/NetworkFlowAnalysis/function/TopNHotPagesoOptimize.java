package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.PageViewCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 实现TopN功能的改进版,主要是为了解决延迟数据(allowedLateness)情况时的触发计算,在这种情况下之前的直接clear清空状态的逻辑时有问题的,这里做出改进
 * @Author: kim
 * @Description:
 * @Date: 9:22 2021/7/7
 * @Version: 1.0
 */
public class TopNHotPagesoOptimize extends KeyedProcessFunction<Long, PageViewCount, String> {

    private static final Logger logger = LoggerFactory.getLogger(TopNHotPagesoOptimize.class);

    private final int topSize;

    // 存储属于同一个窗口的聚合结果, key: url     value: 聚合结果封装类
    private transient MapState<String, PageViewCount> pageViewCountMapState;



    public TopNHotPagesoOptimize(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pageViewCountMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, PageViewCount>("pageViewCountMapStateDesc",
                        TypeInformation.of(String.class), TypeInformation.of(new TypeHint<PageViewCount>() {})));
    }


    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
        if (pageViewCountMapState.contains(value.getUrl())) {
        }
        pageViewCountMapState.put(value.getUrl(), value);
        // 注册一个50ms的定时器,触发排序
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 50L);

        // 注意这里注册了另外一个定时器,它的作用是配合延迟(allowedLateness),所以定时器时间要和延迟时间(allowedLateness)一致,当到达延迟时间后,要把之前该窗口中的状态数据清空
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        // 这里需要判断定时器触发时到底是到达清理状态还是触发排序
        if (timestamp == ctx.timestamp() + 60 * 1000L) {
            // 清空状态,释放内存
            pageViewCountMapState.clear();
            return;
        }

        // 取出同一个窗口结束日期的所有页面的点击量
        ArrayList<Map.Entry<String, PageViewCount>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

        pageViewCounts.sort(new Comparator<Map.Entry<String, PageViewCount>>() {
            @Override
            public int compare(Map.Entry<String, PageViewCount> o1, Map.Entry<String, PageViewCount> o2) {
                return o2.getValue().getViewCount() > o1.getValue().getViewCount() ? 1 : (o2.getValue().getViewCount() < o1.getValue().getViewCount() ? -1 : 0);
            }
        });

        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("=======================================================\n");
        resultBuilder.append("窗口的结束时间: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");
        // 取出前Top N
        for (int i = 0; i < Math.min(pageViewCounts.size(), topSize); i++) {
            PageViewCount currentPageViewCount = pageViewCounts.get(i).getValue();
            resultBuilder.append("NO ")
                    .append(i+1)
                    .append(":")
                    .append(" 页面URL = ")
                    .append(currentPageViewCount.getUrl())
                    .append(" 点击量 = ")
                    .append(currentPageViewCount.getViewCount())
                    .append("\n");
        }
        resultBuilder.append("=======================================================\n");

        TimeUnit.SECONDS.sleep(1L);

        out.collect(resultBuilder.toString());
    }
}
