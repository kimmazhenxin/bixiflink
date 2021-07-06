package HotItemAnalysis.function;

import HotItemAnalysis.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 自定义增量聚合函数,
 * COUNT 统计的聚合函数实现，每出现一条记录加一
 * @Author: mazhenxin
 * @File: ItemCountAgg.java
 * @Date: 2021/07/03 16:04
 */
public class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    public ItemCountAgg() {
    }

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
