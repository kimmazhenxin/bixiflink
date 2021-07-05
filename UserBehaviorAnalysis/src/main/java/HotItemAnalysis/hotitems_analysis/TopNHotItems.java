package HotItemAnalysis.hotitems_analysis;

import HotItemAnalysis.hotitems.ItemViewCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
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
    private transient ListState<ItemViewCount> listState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemState-state", TypeInformation.of(new TypeHint<ItemViewCount>() {})));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 1.每来一条数据存入ListState中
        listState.add(value);
        // 2. 注册定时器100ms,由于对Key来说,窗口的结束时间戳一致,实际这里只是注册了一个(重复注册无效)
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        // 定时器触发当前已收集到所有数据,TopN排序输出
        Iterable<ItemViewCount> itemViewCounts = listState.get();
        Iterator<ItemViewCount> iterator = itemViewCounts.iterator();
        ArrayList<ItemViewCount> itemViewCountsList = Lists.newArrayList(iterator);

        // 排序
        itemViewCountsList.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return o2.getViewCount() > o1.getViewCount() ? 1 : (o2.getViewCount() == o1.getViewCount() ? 0 : -1);
            }
        });


    }
}
