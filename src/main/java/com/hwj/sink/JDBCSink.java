package com.hwj.sink;

import com.hwj.entity.SensorReading;
import com.hwj.source.FileSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: hwj
 * @CreateTime: 2022-08-19  16:57
 * @Version: 1.0
 * @Description: 自定义Sink--连接MySQL
 */
public class JDBCSink {

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

        //输出到MySQL
        dataStream.addSink(new MyRichSinkFunction());

        //执行
        env.execute();
    }

    public static class MyRichSinkFunction extends RichSinkFunction<SensorReading> {

        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {

            connection = DriverManager.getConnection("jdbc:mysql://hwjaliyun:3306/flink_test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "hwj", "hwjaliyunmysqlmm");
            insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {

                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }

        }

        @Override
        public void close() throws Exception {

            insertStmt.close();
            updateStmt.close();
            connection.close();

        }
    }

}
