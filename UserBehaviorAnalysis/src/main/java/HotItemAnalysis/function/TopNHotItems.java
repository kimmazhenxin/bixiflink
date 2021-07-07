package HotItemAnalysis.function;

import HotItemAnalysis.bean.ItemViewCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口结束的时间戳，输出为 TopN 的结果字符串
 * @Author: mazhenxin
 * @File: TopNHotItems.java
 * @Date: 2021/07/03 16:04
 */
public class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

    private final int topSize;


    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private transient ListState<ItemViewCount> itemListState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemListState = getRuntimeContext().getListState(
                new ListStateDescriptor<ItemViewCount>("itemStateDesc", TypeInformation.of(new TypeHint<ItemViewCount>() {})));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 1.每来一条数据存入ListState状态中
        itemListState.add(value);
        // 2. 注册WindowEnd() + 100的EventTime Timer,定时器100ms
        // 当触发时，说明收齐了属于windowEnd窗口的所有商品数据,触发后续的排序功能
        // 由于对Key来说,窗口的结束时间戳一致,实际这里只是注册了一个(重复注册无效),Flink 框架会自动忽略同一时间的重复注册
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发当前已收集到所有数据,TopN排序输出
        Iterable<ItemViewCount> itemViewCounts = itemListState.get();
        Iterator<ItemViewCount> iterator = itemViewCounts.iterator();
        // 取出同一个窗口结束日期的所有商品的点击量
        ArrayList<ItemViewCount> itemViewCountsList = Lists.newArrayList(iterator);

        // 排序(逆序)
        itemViewCountsList.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.getViewCount() - o1.getViewCount());
                //return o2.getViewCount() > o1.getViewCount() ? 1 : (o2.getViewCount() == o1.getViewCount() ? 0 : -1);
            }
        });

        // 提前清除状态,提前释放内存
        itemListState.clear();

        // 输出统计结果
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("=======================================================\n");
        resultBuilder.append("窗口的结束时间: ").append(new Timestamp(ctx.getCurrentKey())).append("\n");
        // 取出前TopN,这里需要判别窗口的元素个数和topN之间的大小关系
        for (int i = 0; i < Math.min(itemViewCountsList.size(), topSize); i++) {
            ItemViewCount currentItemViewCount = itemViewCountsList.get(i);
            resultBuilder.append("NO ")
                    .append(i+1)
                    .append(":")
                    .append(" 商品ID = ")
                    .append(currentItemViewCount.getItemId())
                    .append(" 热门度 = ")
                    .append(currentItemViewCount.getViewCount())
                    .append("\n");
        }
        resultBuilder.append("=======================================================\n");

        // 控制输出频率，模拟实时滚动结果
        TimeUnit.MILLISECONDS.sleep(1000L);

        // 输出统计数据
        out.collect(resultBuilder.toString());
    }
}
