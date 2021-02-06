package com.kim.nio;


import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

/**
 * 一、使用 NIO完成网络通信的三个核心：
 * 1. 通道（Channel）：负责连接
 *  java.nio.channels.Channel 接口：
 *      | --SelectableChannel
 *          |--SocketChannel
 *          |--ServerSocketChannel
 *          |--DatagramChannel
 *
 *          |--Pipe.SinkChannel
 *          |--Pipe.SourceChannel
 *
 * 2. 缓冲区（Buffer）：负责数据的存取
 *
 * 3. 选择器（Selector）:是 SelectableChannel 的多路复用器。用于 SelectableChannel 监控的IO状况。
 *
 *
 * @Author: kim
 * @Date: 2021/2/5 16:30
 * @Version: 1.0
 */


public class TestNonBlockingNIO {

    //客户端
    public void client(String ip, int port) {
        SocketChannel socketChannel = null;
        FileChannel inputChannel = null;
        try {




        } catch (Exception e) {




        } finally {

        }

    }



    //服务端
    public void server(int port) {

    }


    public static void main(String[] args) {

    }



}
