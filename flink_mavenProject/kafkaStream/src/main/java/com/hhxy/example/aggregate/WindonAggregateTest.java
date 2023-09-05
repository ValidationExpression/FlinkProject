package com.hhxy.example.aggregate;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 使用aggregate窗口函数,统计一个网站的UV和PV的数据,和pv/uv =得出平均用户的访问量
 * UV: 用户访问网站的数据量,数据需要进行去重
 * PV: 用户访问网站的数据量
 */
public class WindonAggregateTest {
    public static void main(String[] args) throws Exception {
        //1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        //2.数据源
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickHouse())
                //乱序流水位线,watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        source.print("data");
        //3.相应的数据运算
        //这里进行开窗,统计pv ,uv的数量
        source.keyBy(date -> true) //这里没有进行分区,(如果大量的数据的时候,可能会出现数据偏移,所以使用分区可以更好的解决这个问题)
                //这里的窗口类型有三种,滑动,滚动,事件.
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new agf())
                .print();

        env.execute();
    }

    //自定义一个aggregateFunction, 用于设置pv和uv的保存数据格式,使用long保存pv的数量, hashSet保存uv
    //这里的字段进行解释, AggregateFunction<Event, Tuple2<Long, HashSet<String>>,Double
    //                                  输入数据, 数据的转换方式,                 输出数据
    public static class agf implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>,Double>{
        //初始化方法,作为数据转换的起始方式.
        @Override  //Tuple2<Long, HashSet<String>>初始值的设置
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L,new HashSet<>());
        }

        //add方法,用于处理每一条传输来的数据,
        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            //这里就是,来一条数据pv+1, 将user存入到hashSet中,去重.
            longHashSetTuple2.f1.add(event.user);  //将用户添加到二元组中
            //将更改的数据,在放入到Tuple2中
            return Tuple2.of(longHashSetTuple2.f0+1,longHashSetTuple2.f1);
        }

        //用于获取自己,需要返回的数据类型
        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            return (double)longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        //用于和并数据
        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
