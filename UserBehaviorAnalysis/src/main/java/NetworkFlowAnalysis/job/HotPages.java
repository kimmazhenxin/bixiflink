package NetworkFlowAnalysis.job;

import NetworkFlowAnalysis.bean.ApacheLogEvent;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 热门网页页面统计,实现 TopN功能
 * 数据源日志的格式:
 * @Author: kim
 * @Description:
 * @Date: 16:30 2021/7/6
 * @Version: 1.0
 */
public class HotPages {

    private static final Logger logger = LoggerFactory.getLogger(HotPages.class);

    public static void main(String[] args) throws Exception{
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据源
        URL fileUrl = HotPages.class.getClassLoader().getResource("UserBehavior.csv");
        DataStreamSource<String> dataSource = env.readTextFile(fileUrl.getPath());
        // 转换
        SingleOutputStreamOperator<ApacheLogEvent> mapStream = dataSource.map(line -> {
            String[] strings = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timestamp = simpleDateFormat.parse(strings[3]).getTime();
            return new ApacheLogEvent(strings[0], strings[1], timestamp, strings[5], strings[6]);
        });
        // 指定 Watermark
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));






        env.execute("Hot Pages Job");
    }


    // 提取EventTime
    public static class TimeStampExtractor implements TimestampAssigner<ApacheLogEvent> {

        public TimeStampExtractor() {
        }

        //指定时间字段作为EventTime
        @Override
        public long extractTimestamp(ApacheLogEvent element, long recordTimestamp) {
            return element.getTimestamp();
        }
    }
}
