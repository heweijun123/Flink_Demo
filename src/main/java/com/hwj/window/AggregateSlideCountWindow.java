package com.hwj.window;

import com.hwj.entity.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-25  17:56
 * @Version: 1.0
 * @Description: 测试滑动计数窗口的增量聚合函数：这里获取每个窗口里的温度平均值
 * 滑动窗口，当窗口不足设置的大小时，会先按照步长输出。
 * eg：窗口大小10，步长2，那么前5次输出时，窗口内的元素个数分别是（2，4，6，8，10），再往后就是10个为一个窗口了。
 */
public class AggregateSlideCountWindow {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(1);

        // 从socket文本流获取数据
        DataStream<String> inputStream = env.socketTextStream("hwjaliyun", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1. 增量聚合函数 (这里获取每个窗口里的温度平均值)
        DataStream<Double> resultStream = dataStream.keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    // 新建的累加器
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {

                        return new Tuple2<>(0d, 0);
                    }

                    // 每个数据在上次的基础上累加
                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {

                        accumulator.setFields(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                        return accumulator;
                    }

                    // 返回结果值
                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {

                        return accumulator.f0 / accumulator.f1;
                    }

                    // 分区合并结果(TimeWindow一般用不到，SessionWindow可能需要考虑合并)
                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {

                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        resultStream.print("result3");

        env.execute();
    }
}
