package com.hhxy.example.text;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) throws Exception {

        //1.创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.数据来源(kafka)--需要添加依赖
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        //还可以设置kafka的group_id ..多个设置
        //flink做为kafka的消费者,有三个参数(topic(主题),反序列化器,kafka连接配置项Properties)
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //3.还有相应的数据转换(如果用到可以在此进行修改对应的数据格式)
        //打印输出
        /**
         * 测试
         * 打开kafka的生产者,创建相应的数据
         * kafka目录下  ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic clicks(主题)
         */
        kafkaStream.print();
        env.execute();
    }
}
