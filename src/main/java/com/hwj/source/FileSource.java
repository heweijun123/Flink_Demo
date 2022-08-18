package com.hwj.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  14:22
 * @Version: 1.0
 * @Description: 从文件中获取数据
 */
public class FileSource {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(1);

        // 从文件中获取数据输出
        String path = FileSource.class.getClassLoader().getResource("").getPath();
        String filePath = path + "sensor.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(filePath);

        //输出
        dataStreamSource.print();

        //执行
        env.execute();
    }
}
