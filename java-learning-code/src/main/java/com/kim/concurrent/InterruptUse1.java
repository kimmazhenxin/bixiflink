package com.kim.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Interrupt的使用
 *  1. 打断阻塞状态的线程（sleep、wait、join），此时的打断标记是 false.
 *  2. 打断正常运行的线程，此时的打算标记是 true, 此时并不意味着立刻结束线程，只是告诉它我要打断它，后续还得通过打断标记来判断处理.
 * @Author: kim
 * @Date: 2021/2/20 08:20
 * @Version: 1.0
 */
public class InterruptUse1 {

    private static final Logger logger = LoggerFactory.getLogger(InterruptUse1.class);


    public static void main(String[] args) throws InterruptedException {

        Thread t1 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                // 阻塞中的线程被打断后状态变为Runnable
                e.printStackTrace();
                logger.info("线程-[{}] 被打断后的状态state: {}", Thread.currentThread().getName(), Thread.currentThread().getState());
                for (int i = 0; i < 10; i++) {
                    System.out.println("i...... " + i);
                }
            }
        }, "t1");
        t1.start();

        TimeUnit.SECONDS.sleep(1);
        logger.info("线程-[t1] 被打断前的状态state: {}", t1.getState());
        t1.interrupt();
        logger.info("打断状态 {}", t1.isInterrupted());
    }



}
