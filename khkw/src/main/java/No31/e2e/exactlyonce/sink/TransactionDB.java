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

    /**
     * 第一阶段提交,将事务id对应的结果数据集
     * @param transactionId
     * @param values
     */
    public void firstPhase(String transactionId, List<Tuple3<String, Long, String>> values) {
        List<Tuple3<String, Long, String>> content = transactionRecords.get(transactionId);
        content.addAll(values);
    }

    /**
     * 第二阶段提交,将事务id对应的结果集真正写入外部存储系统中
     * 注意：这里的逻辑要尽量超级简单,最好是外部一个指令才好,时间和稳定性要求非常高
     * @param transactionId
     */
    public void secondPhase(String transactionId) {
        logger.error(String.format("Persist current transaction...[%s] records...", transactionId));
        List<Tuple3<String, Long, String>> content = transactionRecords.get(transactionId);
        if (null == content) {
            return;
        }
        content.forEach(this::print);

        // 提醒大家,下面这行非常重要,因为NotifyCheckpoint 和 InitializeState(即Recovery)都会调用.
        // 切记: 真正写完以后一定要把事务id对应的临时数据清理掉,否则会出现重复数据
        // 原因是:
        //      1. commit()方法会在两个地方被调用,一个是真正完成CP后notify调用完成数据的真正写入,如果写入正常的话,这一步是没有问题的
        //      2. 如果上述写入失败,这就涉及到之前提到将事务id也存入State是为了防止在CP完成后调用notify真正写入时,由于不受Flink框架控制,可能造成写入失败.
        //         这时候在进行下次recovery时候也会调用commit()方法(具体会在initializeState中调用),会根据事务id进行重试再次提交数据。如果不删除的话,会造成数据重复写
        removeTable("Notify or Recovery", transactionId);
        logger.error(String.format("Persist current transaction...[%s] records...[SUCCESS]", transactionId));
    }

    private void print(Tuple3<String, Long, String> record) {
        logger.error(record.toString());
    }


    /**
     * 移除事务对应的数据集映射
     * 注意：这个非常重要,在Notify 和 Recovery都会调用
     * @param who
     * @param transactionId
     */
    public void removeTable(String who, String transactionId) {
        logger.error(String.format("[%s], Remove table for transaction...[%s]", who, transactionId));
        transactionRecords.remove(transactionId);
    }
}
