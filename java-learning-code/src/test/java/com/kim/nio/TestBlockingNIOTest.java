package com.kim.nio;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

public class TestBlockingNIOTest {

    @Test
    public void client() {
        SocketChannel socketChannel = null;
        FileChannel inputChannel = null;
        try {
            //1.获取通道
            socketChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 6669));
            inputChannel = FileChannel.open(Paths.get("src/main/resources/1.PNG"), StandardOpenOption.READ);

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

    @Test
    public void server() {
        ServerSocketChannel severSocketChannel = null;
        FileChannel outputChannel = null;
        SocketChannel accept = null;
        try {
            //1.获取通道
            severSocketChannel = ServerSocketChannel.open();
            outputChannel = FileChannel.open(Paths.get("src/main/resources/3.PNG"),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            //2.绑定连接
            severSocketChannel.bind(new InetSocketAddress(6669));

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
}