package No31.e2e.exactlyonce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.e2e.exactlyonce.functions
 * 功能描述: 本测试核心是演示流计算语义 at-most-once, at-least-once, exactly-once, e2e-exactly-once.
 * 操作步骤: 1. 直接运行程序，观察atMostOnce语义效果；
 *         2. 打开atLeastOnce(env)，观察atLeastOnce效果,主要是要和exactlyOnce进行输出对比。
 *         3. 打开exactlyOnce(env)，观察exactlyOnce效果，主要是要和atLeastOnce进行输出对比。
 *         4. 打开exactlyOnce2(env)，观察print效果(相当于sink），主要是要和e2eExactlyOnce进行输出对比。
 *         5. 打开e2eExactlyOnce(env)，观察print效果(相当于sink），主要是要和exactlyOnce2(env)进行输出对比。
 * @Author: kim
 * @Date: 2020/12/5 16:39
 * @Version: 1.0
 */
public class E2eExactlyOnceTestCase {

	private  static  final  Logger logger = LoggerFactory.getLogger(E2eExactlyOnceTestCase.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.enableCheckpointing(1000L);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)));



		atMostOnce(env);





		env.execute("E2E-Exactly-Once");
	}



	/**
	 * 模拟无状态的数据源，同时数据是根据时间的推移而产生的，所以一旦
	 * 流计算过程发生异常，那么异常期间的数据就丢失了，也就是at-least-once。
	 */
	private static  void atMostOnce(StreamExecutionEnvironment env) {
		DataStreamSource<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
			@Override
			public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
				while (true) {
					ctx.collect(new Tuple2<>("key", System.currentTimeMillis()));
					Thread.sleep(500L);
				}
			}

			@Override
			public void cancel() {

			}
		});

		source.map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
			@Override
			public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
				if (value.f1 % 10 == 0) {
					String msg = String.format("Bad data [%d]", value.f1);
					throw new RuntimeException(msg);
				}
				return value;
			}
		}).print();
	}


	private static void atLeastOnce(StreamExecutionEnvironment env) {




	}





	private static KeyedStream basicLogic(StreamExecutionEnvironment env) {
		return null;
	}

}
