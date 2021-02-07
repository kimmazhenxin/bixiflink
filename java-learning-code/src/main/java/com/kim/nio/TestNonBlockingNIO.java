package com.kim.nio;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

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

    private static final Logger logger = LoggerFactory.getLogger(TestNonBlockingNIO.class);



    //客户端
    public void client(String ip, int port) {
        SocketChannel socketChannel = null;
        FileChannel inputChannel = null;
        try {
            //1. 获取通道
            socketChannel = SocketChannel.open(new InetSocketAddress(ip, port));

            //2. 切换非阻塞模式
            socketChannel.configureBlocking(false);

            //3. 分配指定大小的缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            //4. 发送数据给服务端
            buffer.put(new Date().toString().getBytes());
            buffer.flip();
            socketChannel.write(buffer);
            buffer.clear();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputChannel != null) {
                try {
                    inputChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }



    //服务端
    public void server(int port) throws IOException {
        //1. 获取通道
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        //2. 切换非阻塞模式
        serverSocketChannel.configureBlocking(false);

        //3. 绑定连接
        serverSocketChannel.bind(new InetSocketAddress(port));

        //4. 获取连接器
        Selector selector = Selector.open();

        //5. 将通道注册到选择器上,并且指定“监听接收事件”
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        //6. 轮训式的获取选择器上已经“准备就绪”的事件
        while (selector.select() > 0) {
            //7. 获取当前选择器中所有注册的“选择键(已就绪的监听事件)”
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                //8. 获取准备“就绪”的事件
                SelectionKey selectionKey = iterator.next();

                //9. 判断具体事件是什么事件
                if (selectionKey.isAcceptable()) {

                    //10. 若“接收就绪”，获取客户端连接
                    SocketChannel accept = serverSocketChannel.accept();

                    //11. 切换非阻塞模式
                    accept.configureBlocking(false);

                    //12. 将该通道注册到选择器上
                    accept.register(selector, SelectionKey.OP_READ);
                } else if (selectionKey.isReadable()) {
                    //13. 获取当前选择器上“读就绪”状态的通道
                    SocketChannel channel = (SocketChannel)selectionKey.channel();

                    //14. 读取数据
                    ByteBuffer buffer = ByteBuffer.allocate(1024);

                    int len = 0;
                    while ((len = channel.read(buffer)) > 0) {
                        buffer.flip();
                        System.out.println(new String(buffer.array(), 0, len));
                        buffer.clear();
                    }
                }
            }

        }


    }


    public static void main(String[] args) {
        HashSet<Long> set = new HashSet<>();
        set.add(1L);
        set.add(2L);
        logger.info("set:{}", set);
        System.out.println(set);
    }



}
