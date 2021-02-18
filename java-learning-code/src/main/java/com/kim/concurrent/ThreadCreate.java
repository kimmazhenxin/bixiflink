package com.kim.concurrent;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * 创建线程的三种方式
 * @Author: kim
 * @Date: 2021/02/18 9:27
 * @Version: 1.0
 */
public class ThreadCreate {
    private static final Logger logger = LoggerFactory.getLogger(ThreadCreate.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1. Thread类
        Thread thread = new Thread("t1") {
            @Override
            public void run() {
                int i = 0;
                while (i < 10) {
                    logger.info("{}: i*i is {}", Thread.currentThread().getName(), i*i);
                    i++;
                }
            }
        };
        thread.start();

        //2. Runnable接口
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                int i = 0;
                while (i < 10) {
                    logger.info("{}: i*i is {}", Thread.currentThread().getName(), i*i);
                    i++;
                }
            }
        };
        //或者lambda函数式写法,前提是必须是函数式接口
        Runnable runnable1 = () -> {
            int i = 0;
            while (i < 10) {
                logger.info("{}: i*i is {}", Thread.currentThread().getName(), i*i);
                i++;
            }
        };
        new Thread(runnable, "t2").start();

        //3. FutureTask类
        FutureTask<Integer> task = new FutureTask<>(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int i = 0;
                while (i < 10) {
                    logger.info("{}: i*i is {}", Thread.currentThread().getName(), i * i);
                    i++;
                }
                return 10;
            }
        });
        new Thread(task, "t3").start();
        // get()方法获取线程返回值
        logger.info("futureTask result is {}", task.get());

        //主线程打印
        logger.info("do some things......");
    }
}
