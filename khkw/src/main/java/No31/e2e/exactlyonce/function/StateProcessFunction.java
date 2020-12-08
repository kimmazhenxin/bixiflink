package No31.e2e.exactlyonce.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * 检验是否是重复消费
 * @Author: kim
 * @Date: 2020/12/7 22:38
 * @Version: 1.0
 */
public class StateProcessFunction
		extends KeyedProcessFunction<String, Tuple3<String, Long, String>, Tuple3<String, Long, String>> {

	private static  final Logger logger = LoggerFactory.getLogger(StateProcessFunction.class);

	private final String STATE_NAME = "processData";

	private transient ListState<Tuple3<String, Long, String>> processData;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		processData = getRuntimeContext().getListState(
				new ListStateDescriptor<>(STATE_NAME, Types.TUPLE(Types.STRING, Types.LONG, Types.STRING)));


	}

	@Override
	public void processElement(Tuple3<String, Long, String> value, Context ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
		//throw new RuntimeException(".........");
		boolean isDuplicate = false;
		Iterator<Tuple3<String, Long, String>> it = processData.get().iterator();
		while (it.hasNext()) {
			if (it.next().equals(value)) {
				isDuplicate = true;
				break;
			}
		}

		// 数据重复就发出,否则存在状态中
		if (isDuplicate) {
			out.collect(value);
		} else {
			processData.add(value);
		}
	}
}
