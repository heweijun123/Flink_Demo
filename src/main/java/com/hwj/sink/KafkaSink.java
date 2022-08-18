package com.hwj.sink;

import com.hwj.entity.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-18  11:11
 * @Version: 1.0
 * @Description: 测试KafkaSink
 */
public class KafkaSink {
    
    public static void main(String[] args) throws Exception{

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hwjaliyun:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // flink添加外部数据源
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        // 序列化从Kafka中读取的数据
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        // 将数据写入Kafka
        dataStream.addSink( new FlinkKafkaProducer<>("hwjaliyun:9092", "sinktest", new SimpleStringSchema()));

        //执行
        env.execute();
    }
}
