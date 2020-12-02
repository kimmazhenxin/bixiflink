package com.kim.window.hotitems;


import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * key选择器,基于商品ID作为key
 * @Author: mazhenxin
 * @File: UserBehaviorKeySelector.java
 * @Date: 2020/11/30 19:31
 */
public class UserBehaviorKeySelector implements KeySelector<UserBehavior, Long> {

    private static final Logger logger = LoggerFactory.getLogger(UserBehaviorKeySelector.class);

    @Override
    public Long getKey(UserBehavior value) throws Exception {
        return value.getItemId();
    }
}
