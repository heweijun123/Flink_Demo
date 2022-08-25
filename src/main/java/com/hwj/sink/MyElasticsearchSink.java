package com.hwj.sink;

import com.hwj.entity.SensorReading;
import com.hwj.source.FileSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.table.Elasticsearch7DynamicSinkFactory;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-18  15:34
 * @Version: 1.0
 * @Description: Sink输出到ES（设置了用户密码鉴权）
 */
public class MyElasticsearchSink {

    public static void main(String[] args) throws Exception {

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
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hwjaliyun", 9200));
        //设置用户密码
        ElasticsearchSink.Builder esSinkBuilder = new ElasticsearchSink.Builder(httpHosts, new MyElasticsearchSinkFunction());
        esSinkBuilder.setRestClientFactory(restClientBuilder ->

                restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {

                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY,
                            new UsernamePasswordCredentials("elastic", "hwjaliyunesmm"));
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }));
        ElasticsearchSink elasticsearchSink = esSinkBuilder.build();
        esSinkBuilder.setBulkFlushMaxActions(1);

        //输出到ES
        dataStream.addSink(elasticsearchSink);
        //执行
        env.execute();
    }

    public static class MyElasticsearchSinkFunction implements ElasticsearchSinkFunction<SensorReading> {


        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            Map<String, String> map = new HashMap<>();
            map.put("id", sensorReading.getId());
            map.put("temperature", sensorReading.getTemperature().toString());
            map.put("timestamp", sensorReading.getTimestamp().toString());
            IndexRequest indexRequest = new IndexRequest("sensor");
            indexRequest.source(map);
            requestIndexer.add(indexRequest);
        }
    }
}
