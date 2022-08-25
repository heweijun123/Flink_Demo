package com.hwj.window;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-25  17:19
 * @Version: 1.0
 * @Description: 测试滚动时间窗口的增量聚合函数：增量聚合函数，特点即每次数据过来都处理，但是到了窗口临界才输出结果。
 */
public class AggregateTimeWindow {

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

        // 开窗测试

        // 1. 增量聚合函数 (这里简单统计每个key组里传感器信息的总数)
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    // 新建的累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 每个数据在上次的基础上累加
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 返回结果值
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 分区合并结果(TimeWindow一般用不到，SessionWindow可能需要考虑合并)
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print("result");

        env.execute();
    }

}
