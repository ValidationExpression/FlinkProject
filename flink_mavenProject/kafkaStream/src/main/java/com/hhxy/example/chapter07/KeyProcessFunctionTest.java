package com.hhxy.example.chapter07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class KeyProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        //1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.数据源
        DataStreamSource<Event> source = env.addSource(new ClickHouse());

        //3.使用keyProcessFunction处理函数,变换数据
        source.keyBy(data -> data.user)
                //这个解释,实现KeyProcessFunction<key的类型,输入的值的类型,输出类型
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        //获取当前的时间
                        //long time = context.timerService().currentProcessingTime();
                        //
                        //collector.collect(context.getCurrentKey()+",数据到达时间:"+new Timestamp(time));
                        //设置一个定时器,在当前时间后的第一个10s触发
                        //context.timerService().registerProcessingTimeTimer(time+10*1000L);

                        //方式2: 测试事件定时器
                        Long timestamp = context.timestamp();
                        collector.collect(context.getCurrentKey() + ",数据时间戳:" + new Timestamp(timestamp));
                        context.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + ",定时器触发时间:" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
