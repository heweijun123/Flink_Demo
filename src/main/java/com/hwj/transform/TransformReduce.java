package com.hwj.transform;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-18  10:30
 * @Version: 1.0
 * @Description: reduce聚合算子使用：获取同组历史温度最高的传感器信息，同时要求实时更新其时间戳信息。
 */
public class TransformReduce {

    public static void main(String[] args) throws Exception {

        // 创建 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 执行环境并行度设置1
        env.setParallelism(1);

        // 从文件中获取数据输出
        String path = TransformBase.class.getClassLoader().getResource("").getPath();
        String filePath = path + "sensor.txt";
        DataStreamSource<String> dataStream = env.readTextFile(filePath);


        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 先分组再聚合
        // 分组
        KeyedStream<SensorReading, String> keyedStream = sensorStream.keyBy(SensorReading::getId);

        //获取同组历史温度最高的传感器信息，同时要求实时更新其时间戳信息。
        //这里有个疑问，如果只有1条数据的时候，reduce方法还执行吗？---不执行
        SingleOutputStreamOperator resultStream = keyedStream.reduce((t1, t2) -> new SensorReading(t1.getId(), t2.getTimestamp(), Math.max(t1.getTemperature(), t2.getTemperature())));


        resultStream.print("result");

        env.execute();
    }
}
