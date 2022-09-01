package com.hwj.entity.com.hwj.watermark;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarksWithIdleness;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-30  10:33
 * @Version: 1.0
 * @Description: 测试Watermark和迟到数据，这里设置的Watermark的延时时间是2s，实际一般设置和window大小一致。
 */
public class EventTimeWindowWM {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink1.12.X 已经默认就是使用EventTime了，所以不需要这行代码
        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(4);

        // 从socket文本流获取数据
        DataStream<String> inputStream = env.socketTextStream("hwjaliyun", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream
                .map(line -> {

                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                })


                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy) )
                // 升序数据设置事件时间和watermark
                //.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                //  @Override
                //  public long extractAscendingTimestamp(SensorReading element) {
                //    return element.getTimestamp() * 1000L;
                //  }
                //});

                // 旧版 (新版官方推荐用assignTimestampsAndWatermarks(WatermarkStrategy) )
                // 乱序数据设置时间戳和watermark
                //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                //      @Override
                //      public long extractTimestamp(SensorReading element) {
                //          return element.getTimestamp() * 1000L;
                //      }
                //  });

                //新版，参考：https://zhuanlan.zhihu.com/p/345655175
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading sensorReading, long l) {

                                return sensorReading.getTimestamp() * 1000L;
                            }
                        }));


        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
