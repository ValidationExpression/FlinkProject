package com.hhxy.example.sinkText01;

import com.hhxy.example.text06.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 实现是从kafka中读取数据,经过flink的处理转换,再次输入到kafka中
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        //1.构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //2.从kafka中读取数据
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        //还可以设置kafka的group_id ..多个设置
        //flink做为kafka的消费者,有三个参数(topic(主题),反序列化器,kafka连接配置项Properties)
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //3.处理数据,将kafka接收的数据进行转换,由String类型转换为Event类型
        SingleOutputStreamOperator<Event> mapResult = kafkaStream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String s) throws Exception {
                //数据转换的方式,mary,./a, 1000的字符串
                //使用,将字符串进行切分操作
                String[] split = s.split(",");
                //将切割后的数据放入到对应的event类型中,请去除空格
                Event event = new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                return event;
            }
        });

        //4.将flink处理后的数据再次写入到kafka中,
        //FlinkKafkaProducer<Event>(传入三个参数1.端口位置,2.主题,3.编码方式(不同的数据,处理比编码的方式不同))
        mapResult.addSink(new FlinkKafkaProducer<Event>("hadoop102:9092", "events", new SerializationSchema<Event>() {
            @Override
            public byte[] serialize(Event event) {
                return new byte[0];
            }
        }));

        //5.启动
        env.execute();

    }
}
