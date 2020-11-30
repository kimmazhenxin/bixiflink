package com.kim.window;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * COUNT 统计的聚合函数实现，每出现一条记录加一
 * @Author: mazhenxin
 * @File: CountAgg.java
 * @Date: 2020/11/30 19:44
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
