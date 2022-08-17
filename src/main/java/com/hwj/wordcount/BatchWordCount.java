package com.hwj.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-17  10:50
 * @Version: 1.0
 * @Description: 批处理 wordcount
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String path = BatchWordCount.class.getClassLoader().getResource("").getPath();
        String filePath = path + "hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(filePath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        AggregateOperator<Tuple2<String, Long>> result = inputDataSet
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);

        //输出
        result.print();

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
