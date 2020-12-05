package com.kim.window.hotitems;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
 * @Author: mazhenxin
 * @File: TopNHotItems.java
 * @Date: 2020/11/30 21:29
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
        listState = getRuntimeContext().getListState(
                new ListStateDescriptor<ItemViewCount>("itemState-state", TypeInformation.of(new TypeHint<ItemViewCount>() {})));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {



    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }
}
