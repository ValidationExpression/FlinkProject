package com.hhxy.example09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;

/**
 * 实现: 两条流进行合并(全量join)
 */
public class KeyedStateTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据流1
        SingleOutputStreamOperator<Tuple3<String, String, Long>> source1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));
        //数据流2
        SingleOutputStreamOperator<Tuple3<String, String, Long>> source2 = env.fromElements(
                Tuple3.of("a", "stream-2", 1000L),
                Tuple3.of("b", "stream-2", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));

        //3.数据合并
        source1.keyBy(data -> data.f0)
                .connect(source2.keyBy(data -> data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //定义列表状态
                    private ListState<Tuple2<String,  Long>> source1State;
                    private ListState<Tuple2<String,  Long>> source2State;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        source1State=getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("source1-list", Types.TUPLE(Types.STRING,Types.LONG)));
                        source2State=getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String,  Long>>("source2-list", Types.TUPLE(Types.STRING,Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {

                        //获取另一条流中的数据,并配对输出
                        for (Tuple2<String, Long> right : source2State.get()) {
                            collector.collect(left.f0+" "+left.f2+"=>"+right);
                        }
                        source1State.add(Tuple2.of(left.f0,left.f2));
                    }


                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        //获取另一条流中的数据,并配对输出
                        for (Tuple2<String, Long> left : source2State.get()) {
                            collector.collect(left+"=>"+right.f0+" "+right.f2);
                        }
                        source1State.add(Tuple2.of(right.f0,right.f2));
                    }
                });
        env.execute();
        
    }
}
