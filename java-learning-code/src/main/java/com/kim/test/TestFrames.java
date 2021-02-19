package com.kim.test;

import java.util.concurrent.TimeUnit;

/**
 * 调试线程栈帧
 * @Author: kim
 * @Date: 2021/02/18 9:27
 * @Version: 1.0
 */
public class TestFrames {

    public static void main(String[] args) throws InterruptedException {
        method1(10);
        TimeUnit.MILLISECONDS.sleep(100);
    }

    public static void method1(int x) {
        int y = x + 1;
        Object n = method2();
        System.out.println(n);
    }

    public static Object method2() {
        Object m = new Object();
        return m;
    }
}
