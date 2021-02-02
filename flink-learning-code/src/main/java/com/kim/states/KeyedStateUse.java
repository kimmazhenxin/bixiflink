package com.kim.states;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.operators.MapDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * KeyedState使用:
 *  1. 每个Key都有对应自己的state
 * @Author: mazhenxin
 * @File: KeyedStateUse.java
 * @Date: 2021/1/25 10:32
 */
public class KeyedStateUse {

    private static Logger logger = LoggerFactory.getLogger(KeyedStateUse.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "10.113.31.198";
        DataStreamSource<String> s1 = env.socketTextStream(hostname, 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream =
                s1.map(m -> Tuple2.of(m, 1)).returns(Types.TUPLE(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapStream.keyBy(r -> r.f0);

        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

            private transient MapState<String, Integer> mapState;

            private transient ValueState<Tuple3<String, Integer, HashSet<Long>>> wordState;


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("mapStateDesc",
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

                ValueStateDescriptor<Tuple3<String, Integer, HashSet<Long>>> valueStateDescriptor =
                        new ValueStateDescriptor<>("", TypeInformation.of(new TypeHint<Tuple3<String, Integer, HashSet<Long>>>() {
                }));

                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                wordState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {




            }


            public void initState(Context ctx, Collector<String> out) {
                try {
                    if (mapState.isEmpty()) {
                        logger.info("Note: MapState current key is " + ctx.getCurrentKey());
                        String[] strings = ctx.getCurrentKey().split(".");
                        mapState.put(strings[0], 1);
                        mapState.put(strings[2], 1);
                        logger.info("Note: MapState current key is " + ctx.getCurrentKey() + "MapState is  " + mapState.toString());
                    }

                    if (wordState.value() == null) {
                        logger.info("Note: ValueState current key is " + ctx.getCurrentKey());
                        wordState.update(Tuple3.of(ctx.getCurrentKey(), 1, new HashSet<Long>()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }



        });


    }


}
