package com.kim.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 两阶段终止模式: 在一个线程 T1 中如何“优雅”终止线程 T2？这里的【优雅】指的是给 T2 一个料理后事的机会。
 * @Author: kim
 * @Date: 2021/2/20 09:20
 * @Version: 1.0
 */



public class TwoPhaseTermination {

    private static final Logger logger = LoggerFactory.getLogger(InterruptUse1.class);

    // 监控线程
    private Thread monitor;

    // 启动监控线程
    public void start() {
        monitor = new Thread(() -> {
            while (true) {
                Thread current= Thread.currentThread();
                if (current.isInterrupted()) {
                    // TODO 被打断后料理后事逻辑
                    logger.info("线程-【{}】料理后事", current.getName());
                    break;
                }

                try {
                    // 注意: 这里有两种情况,即阻塞状态时被打断或者情况2执行正常的监控逻辑时被打断,两者得到的打断标记是不一样的
                    TimeUnit.SECONDS.sleep(2);  //情况1
                    // TODO 执行监控业务逻辑记录
                    logger.info("执行监控记录"); // 情况2
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    // 这里有个技巧,阻塞中的线程被打断后会进入Runnable状态,这时候重新设置打算标记后就为true了
                    current.interrupt();
                }

            }
        }, "monitor");
        monitor.start();
    }

    // 启动中断线程
    public void stop() {
        monitor.interrupt();
    }


    public static void main(String[] args) throws InterruptedException {
        TwoPhaseTermination termination = new TwoPhaseTermination();
        termination.start();


        TimeUnit.SECONDS.sleep(5);
        termination.stop();
    }

}




