package com.hwj.transform;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-18  11:03
 * @Version: 1.0
 * @Description: 测试富函数
 */
public class RichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String path = TransformBase.class.getClassLoader().getResource("").getPath();
        String filePath = path + "sensor.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filePath);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();
    }

    // 传统的Function不能获取上下文信息，只能处理当前数据，不能和其他数据交互
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义富函数类（RichMapFunction是一个抽象类）
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
//            RichFunction可以获取State状态
//            getRuntimeContext().getState();
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}
