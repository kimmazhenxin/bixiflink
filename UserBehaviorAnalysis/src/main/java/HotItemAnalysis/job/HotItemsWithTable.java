package HotItemAnalysis.job;

import HotItemAnalysis.bean.UserBehavior;
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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用Table API实现TopN功能
 * @Author: kim
 * @Description:
 * @Date: 10:17 2021/7/6
 * @Version: 1.0
 */
public class HotItemsWithTable {

    private static final Logger logger = LoggerFactory.getLogger(HotItemsWithTable.class);

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 指定使用BlinkPlanner
        EnvironmentSettings envSetting = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        // 创建Flink SQL运行时环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSetting);


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

        // 使用Table API实现TOP N========================================================================
        // 4. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(filterStream, "itemId, behavior, timestamp.rowtime as ts");

        // 5. 分组开窗, Table API
        Table windowAggTable = dataTable
                .filter($("behavior").isEqual("pv"))
                // 或者这种写法: window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .window(Slide
                        .over(ExpressionParser.parseExpression("1.hours"))
                        .every(ExpressionParser.parseExpression("5.minutes"))
                        .on(ExpressionParser.parseExpression("ts"))
                        .as("w"))
                .groupBy($("itemId"), $("w"))   // 按照商品ID、窗口结束时间分组
                // 或者这种写法: select("itemId, w.end as windowEnd, itemId.count as cnt")
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

        // 6. 利用SQL的开窗函数,对count值进行排序并获取 Row Number,获取 Top N
        // Table 转换成 DataStream, 这里需要注意的是滑动窗口的Table本质是一个AppendStream,并不是 RetractStream!!
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        // 创建视图方便后面执行SQL
        tableEnv.createTemporaryView("agg_table", aggStream, $("itemId"), $("windowEnd"), $("cnt"));

        // 执行开窗SQL,取 Top N
        Table resultTable = tableEnv.sqlQuery("select * from " +
                "(select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg_table) " +
                "where row_num <=5");

        // 转换成DataStream,这里需要注意的是此时的Table开了窗口函数之后,它本质是 RetractStream,和上面又不太一样了!!
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute("Hot Items Job with Table/SQL");
    }
}
