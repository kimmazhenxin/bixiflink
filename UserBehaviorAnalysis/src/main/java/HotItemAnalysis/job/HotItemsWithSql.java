package HotItemAnalysis.job;

import HotItemAnalysis.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.net.URL;
import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用纯SQL实现TopN功能
 * @Author: kim
 * @Description:
 * @Date: 15:31 2021/7/6
 * @Version: 1.0
 */
public class HotItemsWithSql {

    private static final Logger logger = LoggerFactory.getLogger(HotItemsWithSql.class);

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

        // 使用纯SQL实现上述的TOP N功能================================================================
        // 4. 将数据源注册视图表
        tableEnv.createTemporaryView("data_table", filterStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        // 5. SQL实现TOP N
        Table sqlResultTable = tableEnv.sqlQuery("select * from " +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                " from " +
                "   (select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "   from data_table " +
                "   where behavior  = 'pv' " +
                "   group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "   )" +
                " ) where row_num <=5 ");
        // 6. 转化成Stream输出
        tableEnv.toRetractStream(sqlResultTable, Row.class).print();


        env.execute("Hot Items Job with SQL");
    }
}
