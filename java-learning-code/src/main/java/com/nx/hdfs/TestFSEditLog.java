package com.nx.hdfs;

/**
 *
 * 测试HDFS中的双缓冲写元数据
 * @Author: kim
 * @Date: 2021/2/25 9:12
 * @Version: 1.0
 */
public class TestFSEditLog {

    public static void main(String[] args) {

        final FSEditLog fsEditLog = new FSEditLog();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 10; j++) {
                        fsEditLog.logEdit("日志信息");
                    }
                }
            }, "" + i);
            thread.start();
        }
    }
}
