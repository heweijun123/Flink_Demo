package com.hwj.sink;

import com.hwj.entity.SensorReading;
import com.hwj.source.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-18  15:34
 * @Version: 1.0
 * @Description: Sink输出到ES
 */
public class MyElasticsearchSink {

    public static void main(String[] args) throws Exception{

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(1);

        // 从文件中获取数据输出
        String path = FileSource.class.getClassLoader().getResource("").getPath();
        String filePath = path + "sensor.txt";
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //输出到Elasticsearch
        new ElasticsearchSink.Builder()
        dataStream.addSink();

        env.execute();
    }
}
