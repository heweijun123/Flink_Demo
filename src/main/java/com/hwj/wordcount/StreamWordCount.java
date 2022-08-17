package com.hwj.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  11:28
 * @Version: 1.0
 * @Description: 流处理 wordcount
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(4);

        // 从文件中读取数据
        //String path = BatchWordCount.class.getClassLoader().getResource("").getPath();
        //String filePath = path + "hello.txt";
        //DataStreamSource<String> dataStreamSource = env.readTextFile(filePath);

        //从socket文本流读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hwjaliyun", 7777);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = dataStreamSource
                .flatMap(new MyFlatMapFunction())
                .keyBy(item->item.f0)
                .sum(1);

        result.print("result");
        // 执行任务
        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {

            for (String item : s.split(" ")) {

                collector.collect(new Tuple2<>(item, 1l));
            }
        }
    }
}
