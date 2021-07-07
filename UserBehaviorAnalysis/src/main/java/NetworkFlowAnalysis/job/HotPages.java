package NetworkFlowAnalysis.job;

import NetworkFlowAnalysis.bean.ApacheLogEvent;
import NetworkFlowAnalysis.bean.PageViewCount;
import NetworkFlowAnalysis.function.PageCountAgg;
import NetworkFlowAnalysis.function.PageCountWindowResult;
import NetworkFlowAnalysis.function.TopNHotPages;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * 热门网页页面统计,实现 TopN功能,，每隔5s输出最近10min中页面的点击量
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

        // 2.读取数据源
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
                WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        // 3.分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream =
                dataStream.filter(data -> "GET".equals(data.getMethod()))
                        .filter(data -> {
                            String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                            return Pattern.matches(regex, data.getUrl());
                        })
                        .keyBy(data -> data.getUrl())
                        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                        //.allowedLateness(Time.minutes(1)) // 允许延迟1min钟
                        .aggregate(new PageCountAgg(), new PageCountWindowResult());


        // 4. 收集同一个窗口count数据,排序输出
        SingleOutputStreamOperator<String> resultStream =
                windowAggStream.keyBy(record -> record.getWindowEnd())
                        .process(new TopNHotPages(5));


        resultStream.print();


        env.execute("Hot Pages Job");
    }

}
