package com.hhxy.example.text05;

import com.hhxy.example.test02.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromReduce {
    public static void main(String[] args) throws Exception {
        //1.构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);
        // 2. 数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L),
                new Event("b", "./ada?id=1", 2002L),
                new Event("c", "./sdf", 3213L),
                new Event("b", "./ada?id=2", 2222L),
                new Event("b", "./ada?id=3", 3212L));

        //3.数据分组,-->实现:统计每一个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clinkByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        //当有一个用户的时候,频次+1 (f0,f1)
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)  //这里的f0就是用户,分组聚合操作,将同一用户的放在一起
                //reduce()方式的聚合,为迭代式.看图reduce迭代方式
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //所以这里使用的是value1.f1+value2.f1
                        return Tuple2.of(value1.f0, value2.f1 + value1.f1);
                    }
                });
        // 4.选取当前最活跃的用户,注意这里使用max/reduce聚合,还需要进行keyBy分组操作,
        // 但是这里的需求是得到所有用户中最大活跃的用户,不能使用user作为分组字段,所以这里自定义分组,
        // 将所有用户放在一个分组中,就可以使用max/reduce获取最大的活跃值
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clinkByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //这里需要进行判断,得到最大的那个活跃值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        //输出
        result.print();
        env.execute();

    }
}
