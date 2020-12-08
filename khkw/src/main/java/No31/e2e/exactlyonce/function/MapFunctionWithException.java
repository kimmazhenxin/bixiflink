package No31.e2e.exactlyonce.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;

/**
 * Map处理,实现CheckpointListener接口,在checkpoint完成后通知
 * @Author: kim
 * @Date: 2020/12/7 22:23
 * @Version: 1.0
 */
public class MapFunctionWithException
		extends RichMapFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>>
		implements CheckpointListener {

	// 延迟时长
	private long delay;

	private String name;

	private transient volatile boolean needFail = false;

	public MapFunctionWithException() {
	}

	public MapFunctionWithException(String name, long delay) {
		this.name = name;
		this.delay = delay;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public Tuple3<String, Long, String> map(Tuple3<String, Long, String> value) throws Exception {
		Thread.sleep(delay);
		if (needFail) {
			throw new RuntimeException(name + " Error for testing......");
		}

		return value;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		this.needFail = true;
		System.err.println(String.format("Source name is [%s], MAP - CP SUCCESS [%d]", this.name, checkpointId));
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		System.err.println(String.format("MAP - CP FAIL    [%d]", checkpointId));
	}
}
