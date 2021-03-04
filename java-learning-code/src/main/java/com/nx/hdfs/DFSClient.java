package com.nx.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 客户端
 */
public class DFSClient {

    public static void main(String[] args) throws IOException {
        //构建客户端(本质就是获取到服务端的代理)
        ClientProtocol namenode = RPC.getProxy(ClientProtocol.class,
                ClientProtocol.versionID,
                new InetSocketAddress("localhost", 9999),
                new Configuration());

        System.out.println("客户端启动......");

        namenode.makeDir("/user/opt/soft");
    }

}
