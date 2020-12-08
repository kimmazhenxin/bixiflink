package No31.e2e.exactlyonce.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @Author: mazhenxin
 * @File: ParallelCheckpointedSource.java
 * @Date: 2020/12/7 9:32
 */
public class ParallelCheckpointedSource
        extends RichParallelSourceFunction<Tuple3<String, Long, String>>
        implements CheckpointedFunction {
    private static  final Logger logger = LoggerFactory.getLogger(ParallelCheckpointedSource.class);

    private static final long serialVersionUID = 1L;

    // 标示数据源一直在读取数据
    private volatile boolean running = true;

    // 数据源的消费offset
    private transient long offset;

    // offsetState
    private  transient ListState<Long> offsetState;

    // offsetState name
    private static final String OFFSET_STATE_NAME = "offset-states";

    // 当前任务实例的编号
    private transient int insexOfTask;

    private String name = "-";

    public ParallelCheckpointedSource() {
    }

    public ParallelCheckpointedSource(String name) {
        this.name = name;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, String>> ctx) throws Exception {
        while (running) {
            // 这是必须添加的线程同步锁，主要是为了和snapshotState进行线程控制
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(new Tuple3<>("key", ++offset, this.name));
            }
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            logger.error("snapshotState() called closed source");
        } else {
          // 清除上次的state
          this.offsetState.clear();
          // 持久化最新的offset
          this.offsetState.add(offset);
          logger.warn(String.format("Source name is [%s], Source CP SUCCESS", name));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        insexOfTask = getRuntimeContext().getIndexOfThisSubtask();

        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<>(OFFSET_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO);

        this.offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);


        // 容错恢复时
        if (context.isRestored()) {
            int listStateSize = 0;
            for (Long offsetValue: this.offsetState.get()) {
                listStateSize++;
                this.offset = offsetValue;
                logger.error(String.format("Current Source [%s] Restore from offset [%d]", this.name, this.offset));
            }
            logger.warn(String.format("name is [%s], listState size is [%d]" ,this.name, listStateSize));
        }
    }
}
