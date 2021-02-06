package com.kim.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

//阻塞式Socket,临时解决办法是使用  socketChannel.shutdownOutput();
public class TsetBlockingNIO2 {

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

            //临时解决办法: 关闭发送,告诉服务器已经发送完毕,否则无法接收到服务端的反馈,服务器端也会卡死,因为服务器不知道客户端有没有发送完数据
            socketChannel.shutdownOutput();

            //接收服务端的反馈
            int len = 0;
            while ((len = socketChannel.read(buffer)) != -1) {
                buffer.flip();
                System.out.println(new String(buffer.array(), 0, len));
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
    public void server(int port) throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        FileChannel outputChannel =
                FileChannel.open(Paths.get("D:/WorkSpace/IDEA/bixiflink/java-learning-code/src/main/resources/3.PNG"),
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        serverSocketChannel.bind(new InetSocketAddress(port));

        SocketChannel accept = serverSocketChannel.accept();

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        while (accept.read(buffer) != -1) {
            buffer.flip();
            outputChannel.write(buffer);
            buffer.clear();
        }

        //发送反馈给客户端
        buffer.put("服务端接收数据成功".getBytes());
        buffer.flip();
        int numBytes = accept.write(buffer);
        System.out.println("发送的字节数: " + numBytes);
    }




    public static void main(String[] args) throws IOException {
        TsetBlockingNIO2 blockingNIO2 = new TsetBlockingNIO2();
        blockingNIO2.server(6669);

    }
}
