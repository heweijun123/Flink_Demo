package com.hwj.source;

import com.hwj.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  16:19
 * @Version: 1.0
 * @Description: 自定义Source
 */
public class UDFSource {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        //输出
        dataStream.print();

        //执行
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 标示位，控制数据产生
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度
            Map<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; ++i) {

                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                // 控制输出频率
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {

            this.running = false;
        }
    }

}
