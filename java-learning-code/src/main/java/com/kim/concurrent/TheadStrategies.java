package com.kim.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 统筹方法案例：即线程方法的使用
 * @Author: kim
 * @Date: 2021/2/20 09:20
 * @Version: 1.0
 */
public class TheadStrategies {

    private static final Logger logger = LoggerFactory.getLogger(InterruptUse1.class);

    public static void main(String[] args) {

        Thread t1 = new Thread(() -> {
            try {
                logger.info("【老王】--洗水壶");
                TimeUnit.SECONDS.sleep(1);
                logger.info("【老王】--烧开水");
                TimeUnit.SECONDS.sleep(6);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "老王");


        Thread t2 = new Thread(() -> {
            try {
                logger.info("【小王】--洗茶壶");
                TimeUnit.SECONDS.sleep(2);
                logger.info("【小王】--洗茶杯");
                TimeUnit.SECONDS.sleep(2);
                logger.info("【小王】--拿茶叶");
                TimeUnit.SECONDS.sleep(1);
                // t2线程必须要等待线程t1烧开水结束后再执行后续逻辑
                t1.join();
                logger.info("【小王】--泡茶");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "小王");

        t1.start();
        t2.start();
    }

}
