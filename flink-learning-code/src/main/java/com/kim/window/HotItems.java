package com.kim.window;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;


/**
 * 读取CSV文件数据,对热门商品统计,每隔5分钟统计过去1小时内点击量最多的前N个商品
 * @Author: mazhenxin
 * @File: HotItems.java
 * @Date: 2020/11/30 16:30
 */
public class HotItems {

    private static final Logger logger = LoggerFactory.getLogger(HotItems.class);

    public static void main(String[] args) throws URISyntaxException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取CSV文件
        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = {"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        //创建数据源,得到 UserBehavior 类型的 DataStream
        DataStreamSource<UserBehavior> csvSource = env.createInput(csvInputFormat);


        SingleOutputStreamOperator<UserBehavior> filterStream = csvSource
                // 抽取出时间生成 watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                // 原始数据单位秒，将其转成毫秒
                return element.getTimestamp() * 1000;
            }})
                //过滤出只有点击行为的数据
                .filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.getBehavior().equals("pv");
            }
        });


        filterStream
                .keyBy(new UserBehaviorKeySelector())
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg());



    }
}
