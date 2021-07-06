package HotItemAnalysis.job;

import HotItemAnalysis.bean.ItemViewCount;
import HotItemAnalysis.bean.UserBehavior;
import HotItemAnalysis.function.ItemCountAgg;
import HotItemAnalysis.function.ItemCountWindowResult;
import HotItemAnalysis.function.TopNHotItems;
import HotItemAnalysis.function.UserBehaviorKeySelector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.net.URL;


/**
 * 读取CSV文件用户行为数据,对热门商品统计,每隔5分钟统计过去1小时内点击量最多的前N个商品,实现TopN
 * @Author: mazhenxin
 * @File: HotItems.java
 * @Date: 2021/07/03 16:04
 */
public class HotItems {

    private static final Logger logger = LoggerFactory.getLogger(HotItems.class);

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取CSV文件,创建数据源
        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = {"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
        //创建数据源,得到 UserBehavior 类型的 DataStream
        DataStreamSource<UserBehavior> csvSource = env.createInput(csvInputFormat, pojoType);


        // 3.指定事件时间和Watermark(这里由于数据已经处理过了,时间时间戳是单调递增的,不存在乱序,直接使用事件时间作为Watermark)
        SingleOutputStreamOperator<UserBehavior> filterStream = csvSource
                // 抽取出时间生成 watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        // 原始数据单位秒，将其转成毫秒
                        return element.getTimestamp() * 1000;
                    }
                })
                //过滤出只有点击行为的数据
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return value.getBehavior().equals("pv");
                    }
                });

        // 4.分组开窗聚合,得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = filterStream
                .keyBy(new UserBehaviorKeySelector())   // 按照商品ID分组
                .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)))  // 滑动窗口
                .aggregate(new ItemCountAgg(), new ItemCountWindowResult()); // 实现聚合并且输出具体的窗口信息

        // 5.以窗口结束时间进行分组,收集同一个窗口的所有商品count数据,排序输出Top N
        DataStream<String> resultStream = windowAggStream
                .keyBy(m-> m.getWindowEnd())    // 以窗口结束时间进行分组
                .process(new TopNHotItems(5));   // 实现 Top N功能

        // 6.输出
        resultStream.print();

        env.execute("Hot Items Job");
    }
}
