package No31.e2e.exactlyonce.sink;


import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.sink
 * 功能描述: 这是e2e Exactly-once 的临时表抽象。
 * @Author: kim
 * @Date: 2020/12/8 23:37
 * @Version: 1.0
 */
public class TransactionTable implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionTable.class);

    private transient  TransactionDB db;
    private final String transactionId;
    private final List<Tuple3<String, Long, String>> buffer = new ArrayList<>();


    public TransactionTable(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TransactionTable insert(Tuple3<String, Long, String> value) {
        initDB();
        // 投产的话,应该逻辑写到远端DB或者文件系统等
        buffer.add(value);
        return this;
    }


    public TransactionTable flush() {
        initDB();
        db.firstPhase(transactionId, buffer);
        return this;
    }

    public void close() {
        buffer.clear();
    }

    private void initDB() {
        if (null == db) {
            db = TransactionDB.getInstance();
        }
    }
}
