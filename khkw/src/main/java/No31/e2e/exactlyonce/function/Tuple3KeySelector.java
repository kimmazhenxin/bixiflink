package No31.e2e.exactlyonce.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * key选择器
 * @Author: kim
 * @Date: 2020/12/7 22:14
 * @Version: 1.0
 */
public class Tuple3KeySelector implements KeySelector<Tuple3<String, Long, String>, String> {

	@Override
	public String getKey(Tuple3<String, Long, String> value) throws Exception {
		return value.f0;
	}
}
