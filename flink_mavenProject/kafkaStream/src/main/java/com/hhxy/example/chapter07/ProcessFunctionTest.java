package com.hhxy.example.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //2.数据源
        SingleOutputStreamOperator<Event> watermarksSource = env.addSource(new ClickHouse())
                //为数据,设置时间线,延迟为0,  这里的时间延迟,一般是为了减少数据在传输过程中的时间耗损,一般会很小.
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                //提取数据中的时间戳,用做时间线
                                return event.timestamp;
                            }
                        }));

        //3.数据转换
        SingleOutputStreamOperator<String> process = watermarksSource.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                //使用处理函数,可以实现大部分的需求
                if (event.user.equals("Tom")) {
                    collector.collect(event.user + "clicks" + event.url);
                }
                collector.collect(event.toString());
                //还可以获取时间戳
                System.out.println("timestamp:" + context.timestamp());
                //获取当前的时间线
                System.out.println("watermark:"+context.timerService().currentWatermark());
            }
        });
        //由于这里是单线程,所以时间线的产生一定是在数据的时间戳之后.一般相差1秒
        /**
         * Event{user='liha', url='/c', timestamp=1694438009291}
         * timestamp:1694438009291----------------------------------------->
         * watermark:1694438008285                                         |
         * Tomclicks/abc@a=1?                                              |
         * Event{user='Tom', url='/abc@a=1?', timestamp=1694438010297}     |
         * timestamp:1694438010297                                         |
         * watermark:1694438009290----------------------------------------->
         */

        process.print();

        env.execute();

    }
}
