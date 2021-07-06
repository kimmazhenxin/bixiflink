package HotItemAnalysis.function;


import HotItemAnalysis.bean.UserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * key选择器,基于商品ID作为key
 * @Author: mazhenxin
 * @File: UserBehaviorKeySelector.java
 * @Date: 2021/07/03 16:04
 */
public class UserBehaviorKeySelector implements KeySelector<UserBehavior, Long> {

    private static final Logger logger = LoggerFactory.getLogger(UserBehaviorKeySelector.class);

    @Override
    public Long getKey(UserBehavior value) throws Exception {
        return value.getItemId();
    }
}
