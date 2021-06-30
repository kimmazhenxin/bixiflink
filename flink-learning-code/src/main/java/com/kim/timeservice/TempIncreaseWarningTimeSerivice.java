package com.kim.timeservice;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 基于定时器实现温度告警:
 * 需求：监控温度传感器的温度值，如果温度值在10 秒钟之内 (processing time)连续上升， 则报警。
 * 数据格式:
 * sensor_1,1605062746810,15.4
 * sensor_1,1605062746820,15.6
 * sensor_1,1605062746830,15.8
 * sensor_6,1605062746840,14.4
 * sensor_1,1605062746850,15.4
 * sensor_6,1605062746860,16.4
 * sensor_1,1605062746810,15.2
 * @Author: kim
 * @Description:
 * @Date: 10:27 2021/6/30
 * @Version: 1.0
 */
public class TempIncreaseWarningTimeSerivice {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        if (args.length != 2) {
            System.out.println("USAGE:\n TempIncreaseWarningTimeSerivice <hostname> <port>");
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        DataStreamSource<String> socketStream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<SensorReading> mapStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                try {
                    String[] strings = value.split(",");
                    if (strings.length !=3) {
                        System.out.println("Input Date format is error");
                        return null;
                    }
                    SensorReading sensor = new SensorReading();
                    sensor.setId(strings[0]);
                    sensor.setTime(Long.valueOf(strings[1]));
                    sensor.setTemperature(Double.valueOf(strings[2]));
                    return sensor;
                } catch (Exception e) {
                    System.out.println(e.getStackTrace());
                }
                return null;
            }
        });

        mapStream.keyBy(r -> r.getId()).process(new MyTempIncreaseWarning(10)).print();

        env.execute("TempIncreaseWarningTimeSerivice");
    }


    public static class MyTempIncreaseWarning extends KeyedProcessFunction<String, SensorReading, String> {
        // 定时器间隔
        private Integer interval;
        // 上一次温度
        private transient ValueState<Double> lastTempState;
        // 上一次时间
        private transient ValueState<Long> lastTimeState;


        public MyTempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTempDesc", Double.class, Double.MIN_VALUE));
            lastTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastTimeDesc", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = this.lastTempState.value();
            Long lastTime = this.lastTimeState.value();

            // 如果温度上升并且没有定时器,注册10秒后的定时器,开始等待
            if (value.getTemperature() > lastTemp && lastTime == null) {
                // 计算定时器时间戳
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
                lastTimeState.update(ts);
                // 如果温度降低且已经有定时器了,那么删除之前注册的定时器未后面重新注册定时器做准备
            } else if (value.getTemperature() < lastTemp && lastTime != null) {
                // 删除之前注册的定时器
                ctx.timerService().deleteProcessingTimeTimer(lastTimeState.value());
                // 清空timer状态
                lastTimeState.clear();
            }

            // 更新状态中的温度
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect( "传感器" + ctx.getCurrentKey() + "的温度连续" + interval + "秒上升" );
            // 报警后需要清空timer状态,为后续设置定时器做准备
            lastTimeState.clear();
        }
    }

}
