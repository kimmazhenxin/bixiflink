package com.nx.hdfs;

/**
 * 服务端协议
 */
public interface ClientProtocol {

    long versionID = 1258L;

    void makeDir(String path);


}
