package No31.e2e.exactlyonce.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.UUID;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述: 端到端的精准一次语义sink示例（测试）
 * TwoPhaseCommitSinkFunction有4个方法:
 * - beginTransaction() Call on initializeState/snapshotState
 * - preCommit() Call on snapshotState
 * - commit()  Call on notifyCheckpointComplete()
 * - abort() Call on close()
 * @Author: kim
 * @Date: 2020/12/8 21:46
 * @Version: 1.0
 */
public class E2EExactlyOnceSinkFunction extends
		TwoPhaseCommitSinkFunction<Tuple3<String, Long, String>, TransactionTable, Void> {

	public E2EExactlyOnceSinkFunction() {
		super(new KryoSerializer<>(TransactionTable.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
	}


	@Override
	protected void invoke(TransactionTable table, Tuple3<String, Long, String> value, Context context) throws Exception {
		table.insert(value);

	}

	/**
	 * Call on initializeState/snapshotState
	 */
	@Override
	protected TransactionTable beginTransaction() throws Exception {
		return TransactionDB.getInstance().createTable(String.format("TransID-[%S]", UUID.randomUUID().toString()));
	}


	/**
	 * Call on snapshotState
	 * 将数据写到临时的存储中
	 */
	@Override
	protected void preCommit(TransactionTable table) throws Exception {
		table.flush();
		table.close();
	}


	/**
	 * Call on notifyCheckpointComplete()
	 * Flink框架真正做完CP的时候来调用这个,把数据真正写入到外部存储系统中
	 * @param table
	 */
	@Override
	protected void commit(TransactionTable table) {
		System.err.println(String.format("SINK - CP SUCCESS [%s]", table.getTransactionId()));
		TransactionDB.getInstance().secondPhase(table.getTransactionId());
	}


	/**
	 * Call on close()
	 * @param table
	 */
	@Override
	protected void abort(TransactionTable table) {
		TransactionDB.getInstance().removeTable("Abort", table.getTransactionId());
		table.close();
	}


}
