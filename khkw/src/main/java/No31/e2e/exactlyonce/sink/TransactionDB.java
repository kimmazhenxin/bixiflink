package No31.e2e.exactlyonce.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.sink
 * 功能描述: 模拟远程存储或者业务本地为了实现端到端精准一次的载体。
 * @Author: mazhenxin
 * @File: TransactionDB.java
 * @Date: 2020/12/9 9:46
 */
public class TransactionDB {
    private static Logger logger = LoggerFactory.getLogger(TransactionDB.class);

    // 事务ID和数据结果集的Mapping映射关系
    private final Map<String, List<Tuple3<String, Long, String>>> transactionRecords = new HashMap<>();

    private static TransactionDB instance;


    private TransactionDB() {

    }


    public static synchronized TransactionDB getInstance() {
        if (instance == null) {
            instance = new TransactionDB();
        }
        return instance;
    }


    /**
     * 创建当前事务的临时存储
     * @param transactionId 事务id
     * @return TransactionTable
     */
    public TransactionTable createTable(String transactionId) {
        logger.error(String.format("Create Table for current transaction...[%s]", transactionId));
        transactionRecords.put(transactionId, new ArrayList<>());
        return new TransactionTable(transactionId);
    }












}
