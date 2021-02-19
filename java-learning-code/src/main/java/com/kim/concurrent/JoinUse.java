package com.kim.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * join的使用
 * 结果：2s
 * @Author: kim
 * @Date: 2021/2/19 22:20
 * @Version: 1.0
 */
public class JoinUse {

	static int r1 = 0;
	static int r2 = 0;

	private static final Logger logger = LoggerFactory.getLogger(JoinUse.class);

	public static void main(String[] args) throws InterruptedException {
		test();
	}

	public static void test() throws InterruptedException {
		Thread t1 = new Thread(() -> {
			try {
				Thread.sleep(1000);
				r1 = 10;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}, "t1");

		Thread t2 = new Thread(() -> {
			try {
				Thread.sleep(2000);
				r2 = 20;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}, "t2");

		long start = System.currentTimeMillis();
		t1.start();
		t2.start();
		t1.join();
		t2.join();
		long end = System.currentTimeMillis();
		logger.info("r1: {} r2: {} cost: {}", r1, r2, end - start);
	}
}
