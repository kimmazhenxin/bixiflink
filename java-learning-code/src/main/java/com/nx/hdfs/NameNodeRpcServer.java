package com.nx.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * 代表RPC的服务端
 * RPC的服务端启动了以后,等待客户端的调用
 */
public class NameNodeRpcServer implements ClientProtocol {

    @Override
    public void makeDir(String path) {
        System.out.println("服务端创建目录成功: " + path);
    }


    public static void main(String[] args) throws IOException {
        //此处构建服务端的设计模式是: 建造者模式
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(ClientProtocol.class)
                .setInstance(new NameNodeRpcServer())
                .build();
        System.out.println("服务端已经启动......");
        server.start();
    }
}
