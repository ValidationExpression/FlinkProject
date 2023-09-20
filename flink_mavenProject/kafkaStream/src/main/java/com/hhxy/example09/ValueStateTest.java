package com.hhxy.example09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 实现:
 * 用于测试,ValueState值状态的应用.
 * 统计用户的pv量,且会进行累加,
 * chapter07进行的测试: 是一种滚动窗口类型的输出,他一个窗口就是一个独立的数据,不会进行累加.
 * 这里的测试: 可以对不同的用户的访问pv进行累加计算,就是每10s计算一次用户的pv数据,并进行输出.
 */
public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.数据源
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickHouse())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        source.print();

        //3.转换数据
        source.keyBy(data -> data.user)
                .process(new UserPVCountResult())
                .print();

        env.execute();
    }

    //自定义实现process的转换类,继承KeyedProcessFunction
                                                                    //key, input, output
    public static class UserPVCountResult extends KeyedProcessFunction<String,Event,String>{
        //定义值状态,用于保存用户的pv数量信息
        ValueState<Long> countState;
        //再次定义要给值值状态,用于保存定时器的状态
        ValueState<Boolean> timeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
            timeState=getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("time",Boolean.class));
        }


        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            //对于每一条到来的数据,在这里进行一次统计
            Long count = countState.value();
            //统计每一个用户的pv值,使用三元运算符,当count的值不为空的时候,pv的数量+1,为空的时候定义一个起始值1.
            countState.update(count==null ? 1 : count+1);

            //判断是否存在定时器状态
            if (timeState.value()==null){
                //此时设置一个定时器,就是当前的时间加上10s
                context.timerService().registerEventTimeTimer(event.timestamp+10*1000L);
                //并更新时间状态
                timeState.update(true);
            }
        }

        //调用方法,用于实现检测定时器的启动

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发一次定时器,这里输出一个结果,
            //ctx.getCurrentKey(): 表示获取到当前的key
            out.collect(ctx.getCurrentKey()+",pv:"+countState.value());
            //这个清空对应的定时器,用于下一次注册
            timeState.clear();
            //这里不再使用,
            //countState.clear();

            //为了将数据更加准确的输出,我们可以将定时器设置在这里,减少了判断的耗时
            ctx.timerService().registerEventTimeTimer(timestamp+10*1000L);
            //并更新时间状态
            timeState.update(true);
        }
    }
}
