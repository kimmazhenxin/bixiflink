package No31.e2e.exactlyonce.sink;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述: 端到端的精准一次语义sink示例（测试）
 * TwoPhaseCommitSinkFunction有4个方法:
 * - beginTransaction() Call on initializeState
 * - preCommit() Call on snapshotState
 * - commit()  Call on notifyCheckpointComplete()
 * - abort() Call on close()
 * @Author: kim
 * @Date: 2020/12/8 21:46
 * @Version: 1.0
 */
public class E2EExactlyOnceSinkFunction {
}
