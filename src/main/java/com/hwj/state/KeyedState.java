package com.hwj.state;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-30  11:52
 * @Version: 1.0
 * @Description: 测试键控状态。一般在算子的open()中声明，因为运行时才能获取上下文信息
 */
public class KeyedState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //// 1. 状态后端配置
        //env.setStateBackend(new MemoryStateBackend());
        //env.setStateBackend(new FsStateBackend("checkpointDataUri"));
        //// 这个需要另外导入依赖
        //env.setStateBackend(new RocksDBStateBackend("checkpointDataUri"));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("hwjaliyun", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {

            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 使用自定义map方法，里面使用 我们自定义的Keyed State
        DataStream<Integer> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .map(new MyMapper());

        resultStream.print("result");
        env.execute();
    }

    // 自定义map富函数，测试 键控状态
    public static class MyMapper extends RichMapFunction<SensorReading,Integer> {

        //        Exception in thread "main" java.lang.IllegalStateException: The runtime context has not been initialized.
        //        ValueState<Integer> valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));

        private ValueState<Integer> valueState;


        // 其它类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
            //            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>())

        }

        @Override
        public void close() throws Exception {

            valueState.clear();
            myListState.clear();
            myMapState.clear();
            //myReducingState.clear();
        }

        // 这里就简单的统计每个 传感器的 信息数量
        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其它状态API调用
            // list state
            for(String str: myListState.get()){
                System.out.println(str);
            }
            myListState.add("hello");
            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");
            // reducing state
            //            myReducingState.add(value);

            myMapState.clear();


            Integer count = valueState.value();
            // 第一次获取是null，需要判断
            count = count==null?0:count;
            ++count;
            valueState.update(count);
            return count;
        }
    }
}

