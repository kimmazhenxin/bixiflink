package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.PageViewCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.concurrent.TimeUnit;

/**
 * 求某个窗口中前 N 名的热门点击网页，key为窗口结束的时间戳，输出为 TopN 的结果字符串
 * @Author: kim
 * @Description:
 * @Date: 17:50 2021/7/6
 * @Version: 1.0
 */
public class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

    private static final Logger logger = LoggerFactory.getLogger(TopNHotPages.class);

    private final int topSize;

    // 存储属于同一个窗口的就和结果
    private transient ListState<PageViewCount> pageViewCountListState;



    public TopNHotPages(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pageViewCountListState = getRuntimeContext().getListState(
                new ListStateDescriptor<PageViewCount>("pageListStateDesc", TypeInformation.of(new TypeHint<PageViewCount>() {})));
    }


    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
        pageViewCountListState.add(value);
        // 注册一个50ms的定时器,触发排序
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 50L);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 取出同一个窗口结束日期的所有页面的点击量
        ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get());

        pageViewCounts.sort(new Comparator<PageViewCount>() {
            @Override
            public int compare(PageViewCount o1, PageViewCount o2) {
                return o2.getViewCount() > o1.getViewCount() ? 1 : (o2.getViewCount() < o1.getViewCount() ? -1 : 0);
            }
        });

        // 清空状态,提前释放内存
        pageViewCountListState.clear();

        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("=======================================================\n");
        resultBuilder.append("窗口的结束时间: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");
        // 取出前Top N
        for (int i = 0; i < Math.min(pageViewCounts.size(), topSize); i++) {
            PageViewCount currentPageViewCount = pageViewCounts.get(i);
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
