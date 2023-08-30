package com.hhxy.example.sinkText01;

import com.hhxy.example.test02.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.TimeUtils;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        //1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //2.添加数据
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L),
                new Event("b", "./ada?id=1", 2002L),
                new Event("c", "./sdf", 3213L),
                new Event("b", "./ada?id=2", 2222L),
                new Event("b", "./ada?id=3", 3212L));

        //3.数据格式化,转换
        //4.数据输出,到文件,需要指定路径和序列化数据类型,(输出到文件的时候,flink采用的是分桶策略进行写入)
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("utf-8"))
                //这个就是对文件进行滚动的设置:就是当文件达到每一个条件的时候就会创建一个新的文件进行接收数据,老的数据进行归档操作.
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //文件的大小限制,此时是1G,当文件达到1G的时候,开启一个新的文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                //时间限制
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                //监测当前不活跃的文件,当一个文件不再活跃的时候,此时可以判定此文件结束,可以进行对文件进行保存归档的操作.
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .build()
                )
                .build();
        //由于上述要输出的文件为string类型的数据,所以此时在这里进行一个文件数据类型转换(这里的转换步骤可以放在4的上面)
        SingleOutputStreamOperator<String> map = stream.map(data -> data.toString());

        //对转换后的数据进行输出,可以调整分区数,将数据均匀的写入到不同的文件中.
        map.addSink(streamingFileSink);

        env.execute();

    }
}
