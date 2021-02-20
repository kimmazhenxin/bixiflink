package com.kim.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Interrupt的使用
 *  1. 打算阻塞状态的线程（sleep、wait、join），此时的打断标记是 false.
 *  2. 打断正常运行的线程，此时的打算标记是 true, 此时并不意味着立刻结束线程，只是告诉它我要打断它，后续还得通过打断标记来判断处理.
 * @Author: kim
 * @Date: 2021/2/20 08:20
 * @Version: 1.0
 */
public class InterruptUse2 {

    private static final Logger logger = LoggerFactory.getLogger(InterruptUse2.class);


    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            while (true) {
                boolean interrupted = Thread.currentThread().isInterrupted();
                // 此时的打算标记为true(非常有用！！！)
                if (interrupted) {
                    logger.info("线程-[{}] 被打断了,退出循环", Thread.currentThread().getName());
                    // TODO 优点是可以做一些后续的收尾处理,然后结束线程......
                    break;
                }
            }
        }, "t1");
        t1.start();

        TimeUnit.SECONDS.sleep(1);
        logger.info("线程-[{}] interrupt", Thread.currentThread().getName());
        t1.interrupt();
    }
}
