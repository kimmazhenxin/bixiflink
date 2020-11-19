package com.kim.file;

import io.debezium.relational.ddl.DdlParserListener;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;

/**
 * Flink数据行式存储
 * @Author: mazhenxin
 * @File: StreamingStoreRowFormatFlie.java
 * @Date: 2020/11/19 10:28
 */
public class StreamingStoreRowFormatFlie {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> stream = env.socketTextStream("", 6666);

        BucketingSink<String> hadoopSink = new BucketingSink<>("file:///user/kim/output/flink/rowformat");
        //设置子目录以及时区
        hadoopSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd-HH", ZoneId.of("Asia/Shanghai")));
        //设置切割方式,按照时间
        hadoopSink.setBatchRolloverInterval(10000L);
        //按照文件大小切割
        hadoopSink.setBatchSize(10 * 1024 * 1024);
        //设置文件前缀
        hadoopSink.setPendingPrefix("kim");
        //设置文件后缀
        hadoopSink.setPendingSuffix(".txt");
        //设置正在写入文件的前缀
        hadoopSink.setInProgressPrefix(".");
        //设置正在写入文件的后缀
        hadoopSink.setInProgressSuffix(".inprogerss");


        stream.addSink(hadoopSink);
        env.execute("flink sink rowformat");
    }
}
