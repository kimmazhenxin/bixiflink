package com.kim.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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
 * @Date: 2021/2/5 9:12
 * @Version: 1.0
 */

public class TestBlockingNIO {

    //客户端
    public void client(String ip, int port) {
        SocketChannel socketChannel = null;
        FileChannel inputChannel = null;
        try {
            //1.获取通道
            socketChannel = SocketChannel.open(new InetSocketAddress(ip, port));
            inputChannel = FileChannel.open(Paths.get("D:/WorkSpace/IDEA/bixiflink/java-learning-code/src/main/resources/1.PNG"), StandardOpenOption.READ);

            //2.分配缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            //3.读取本地文件,并发送到服务端
            while(inputChannel.read(buffer) != -1) {
                buffer.flip();
                socketChannel.write(buffer);
                buffer.clear();
            }
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
    public void server(int port) {
        ServerSocketChannel severSocketChannel = null;
        FileChannel outputChannel = null;
        SocketChannel accept = null;
        try {
            //1.获取通道
            severSocketChannel = ServerSocketChannel.open();
            outputChannel = FileChannel.open(Paths.get("D:/WorkSpace/IDEA/bixiflink/java-learning-code/src/main/resources/3.PNG"), StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            //2.绑定连接
            severSocketChannel.bind(new InetSocketAddress(port));

            //3.获取客户端连接的通道
            accept = severSocketChannel.accept();

            //4.分配指定大小的缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            //5. 接受客户端的数据，并保存到本地
            while (accept.read(buffer) != -1) {
                buffer.flip();
                outputChannel.write(buffer);
                buffer.clear();
            }
        } catch (Exception e) {
            System.out.println("产生异常......");
            e.printStackTrace();
        } finally {
            if (severSocketChannel != null) {
                try {
                    severSocketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputChannel != null) {
                try {
                    outputChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (accept != null) {
                try {
                    accept.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public static void main(String[] args) throws Exception {

        TestBlockingNIO blockingNIO = new TestBlockingNIO();
        blockingNIO.server(6669);
    }
}
