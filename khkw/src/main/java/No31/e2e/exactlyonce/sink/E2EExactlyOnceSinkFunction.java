package No31.e2e.exactlyonce.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

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
	protected void invoke(TransactionTable transaction, Tuple3<String, Long, String> value, Context context) throws Exception {

	}

	@Override
	protected TransactionTable beginTransaction() throws Exception {
		return null;
	}

	@Override
	protected void preCommit(TransactionTable transaction) throws Exception {

	}

	@Override
	protected void commit(TransactionTable transaction) {

	}

	@Override
	protected void abort(TransactionTable transaction) {

	}
}
