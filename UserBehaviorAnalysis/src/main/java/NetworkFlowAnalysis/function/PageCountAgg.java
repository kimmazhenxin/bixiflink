package NetworkFlowAnalysis.function;

import NetworkFlowAnalysis.bean.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现累加器功能
 * @Author: kim
 * @Description:
 * @Date: 17:39 2021/7/6
 * @Version: 1.0
 */
public class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

    private static final Logger logger = LoggerFactory.getLogger(PageCountAgg.class);


    public PageCountAgg() {
    }

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent value, Long accumulator) {
        return accumulator + 1L;
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
