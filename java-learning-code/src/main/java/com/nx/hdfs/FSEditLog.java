package com.nx.hdfs;


import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 *
 * 模拟HDFS中的双缓冲写元数据方案
 * @Author: kim
 * @Date: 2021/2/25 9:12
 * @Version: 1.0
 */

public class FSEditLog {

    private long txid = 0L;

    private DoubleBuffer editLogBuffer = new DoubleBuffer();

    //标记: 当前是否在网磁盘写数据
    private volatile Boolean isSyncRunning = false;

    //标记: 等待同步
    private volatile Boolean isWaitSync = false;

    //记录同步内存里面最大的日志编号
    private volatile Long syncMaxTxid = 0L;

    //一个线程 就会有自己一个ThreadLocal的副本,记录编号id
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();


    /**
     * 记录元数据日志方法
     * @param content   元数据内容
     */
    public void logEdit(String content) {
        //假设依次有线程一、二、三、四、五
        //加锁
        synchronized (this) {
            /**
             * 编号不能重复，是递增的
             * 线程一：1
             * 线程二：2
             * 线程三：3
             *
             * 线程四：4
             * 线程五：5
             */
            txid++;
            System.out.println("线程-"+Thread.currentThread().getName()+"拿到了logEdit的锁,此时日志编号 " + txid  + ", time:         " + System.currentTimeMillis());

            /**
             * 线程一：localTxid = 1
             * 线程二：localTxid = 2
             * 线程三：localTxid = 3
             */
            localTxid.set(txid);
            EditLog editLog = new EditLog(txid, content);
            //写入到内存
            editLogBuffer.write(editLog);
        }//释放锁

        // 同步元数据到磁盘
        logSync();
    }


    /**
     * 同步元数据到磁盘
     */
    public void logSync() {

        synchronized (this) {
            System.out.println("线程-"+Thread.currentThread().getName()+"拿到了logSync()的锁1,此时日志编号 " + txid  + ", time:      " + System.currentTimeMillis());
            if (isSyncRunning) {
                Long txid = localTxid.get();

                if (txid <= syncMaxTxid) {
                    return;
                }

                if (isWaitSync) {
                    return;
                }

                isWaitSync = true;

                while (isSyncRunning) {
                    try {
                        //线程四就在这儿等：1）要么就时间到了 2）被人唤醒了
                        System.out.println("线程-"+Thread.currentThread().getName()+"即将等待wait 2S......");
                        wait(2000); //wait操作是会释放锁
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                isWaitSync = false;
            }

            //进行内存交换。在源码里面是有个判断条件，达到一定地方才会交换内存。
            editLogBuffer.setReadToSync();


            if (editLogBuffer.syncBuffer.size() > 0) {
                syncMaxTxid = editLogBuffer.getSyncMaxTxid();
            }

            isSyncRunning = true;
        } //线程 释放锁

        /**
         * 分段加锁
         * 线程一 刷写数据
         * 这个过程要稍微慢一些，因为往磁盘上面去写数据。
         * 线程一就会在这儿运行一会儿。
         */
        editLogBuffer.flush();

        //重新加锁
        synchronized (this) {
            System.out.println("线程-"+Thread.currentThread().getName()+"拿到了logSync()的锁2,此时日志编号 " + txid  + ", time:       " + System.currentTimeMillis());
            // 线程一 赋值为false
            isSyncRunning = false;
            // 唤醒等待线程
            notify();
        }
    }




    /**
     * 面向对象思想,把每一条元数据信息,都看做是一个对象
     */
    class EditLog {

        private long txid;  //每一条元数据都有唯一编号,顺序递增的

        private String content;     //元数据的内容,比如指令 mkdir /data | delet /data


        public EditLog(long txid, String content) {
            this.txid = txid;
            this.content = content;
        }

        @Override
        public String toString() {
            return "EditLog{" +
                    "txid=" + txid +
                    ", content='" + content + '\'' +
                    '}';
        }
    }




    /***
     * 双缓冲方案
     */
    class DoubleBuffer {

        //第一个内存：接收元数据写入
        LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();

        //第二个内存：把数据写到磁盘
        LinkedList<EditLog> syncBuffer    = new LinkedList<EditLog>();


        /**
         * 接收元数据写入
         * @param editLog
         */
        public void write(EditLog editLog) {
            currentBuffer.add(editLog);
        }


        /**
         * 内存1和内存2交换
         */
        public void setReadToSync() {
            LinkedList<EditLog> tmp = this.currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = tmp;
            System.out.println("线程-" + Thread.currentThread().getName() + "执行currentBuffer和 syncBuffer内存交换" + " :       " + System.currentTimeMillis());
        }

        /**
         * 获取同步内存里面最大的一个日志编号
         * @return
         */
        public Long getSyncMaxTxid() {
            return syncBuffer.getLast().txid;
        }


        /**
         * 将数据刷写到磁盘
         */
        public void flush() {
            for (EditLog editLog : syncBuffer) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println("线程-" + Thread.currentThread().getName() +" 存入磁盘日志信息: " + editLog + " ,此时日志编号 " + txid + ", time:       " + System.currentTimeMillis());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //写完后清空同步缓存为下次写做准备
            syncBuffer.clear();
        }
    }

}
