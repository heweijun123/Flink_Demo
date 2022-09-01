package com.hwj.processfunction;

import com.hwj.entity.SensorReading;
import com.hwj.util.DateUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author: hwj
 * @CreateTime: 2022-09-01  17:31
 * @Version: 1.0
 * @Description: 测试KeyedProcessFunction，设置一个获取数据后第5s给出提示信息的定时器。
 */
public class MyKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("hwjaliyun", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy(SensorReading::getId)
                .process(new MyKeyedProcess())
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class MyKeyedProcess extends KeyedProcessFunction<String, SensorReading, Integer> {
        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            // Timestamp of the element currently being processed or timestamp of a firing timer.
            Long timestamp = ctx.timestamp();
            // Get key of the element being processed.
            String currentKey = ctx.getCurrentKey();
            //            ctx.output();
            long l = ctx.timerService().currentProcessingTime();
            System.out.println(DateUtil.dateTimeFormat(new Timestamp(l))+" 收到数据");
            long l1 = ctx.timerService().currentWatermark();

            // 在5处理时间的5秒延迟后触发
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);
            //            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
            // 删除指定时间触发的定时器
            //            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(DateUtil.dateTimeFormat(new Timestamp(timestamp)) + " 定时器触发");
            ctx.getCurrentKey();
            //            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
