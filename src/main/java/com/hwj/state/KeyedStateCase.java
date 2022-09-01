package com.hwj.state;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: hwj
 * @CreateTime: 2022-09-01  17:04
 * @Version: 1.0
 * @Description: 键控状态的例子：假设做一个温度报警，如果一个传感器前后温差超过10度就报警。这里使用键控状态Keyed State + flatMap来实现
 */
public class KeyedStateCase {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度 = 1
        env.setParallelism(1);

        //// 1. 状态后端配置
        //env.setStateBackend(new MemoryStateBackend());
        //env.setStateBackend(new FsStateBackend("checkpointDataUri"));
        //// 这个需要另外导入依赖
        //env.setStateBackend(new RocksDBStateBackend("checkpointDataUri"));

        // 从socket获取数据
        DataStream<String> inputStream = env.socketTextStream("hwjaliyun", 7777);
        // 转换为SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .flatMap(new MyFlatMapper(10.0));

        resultStream.print();

        env.execute();
    }

    public static class MyFlatMapper extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold = 10.0d;

        private ValueState<Double> lastTemperature;

        public MyFlatMapper(Double threshold) {

            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemperature", Double.class));
        }

        @Override
        public void close() throws Exception {

            lastTemperature.clear();
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {

            if (lastTemperature.value() != null && Math.abs(sensorReading.getTemperature() - lastTemperature.value()) >= threshold) {

                collector.collect(new Tuple3<>(sensorReading.getId(), lastTemperature.value(), sensorReading.getTemperature()));
            }
            lastTemperature.update(sensorReading.getTemperature());
        }
    }
}
