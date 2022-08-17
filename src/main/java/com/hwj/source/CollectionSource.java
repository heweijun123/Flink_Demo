package com.hwj.source;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  14:15
 * @Version: 1.0
 * @Description: 从集合中获取数据
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(4);

        // Source: 从集合Collection中获取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //输出
        dataStream.print("SENSOR");
        intStream.print("INT");

        //执行
        env.execute();
    }
}
