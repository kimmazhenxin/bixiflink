package com.kim.test;

public class Test {
    public static void main(String[] args) {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {

                }
            }
        }, "t1").start();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {

                }
            }
        }, "t2").start();
    }
}
