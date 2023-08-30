package com.hhxy.example.test04;

import com.hhxy.example.test02.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合算子的使用
 */
public class TransfromSimpleAgg {
    public static void main(String[] args) throws Exception {
        //1.构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);
        // 3.数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L),
                new Event("b","./ada?id=1",2002L),
                new Event("c", "./sdf", 3213L),
                new Event("b","./ada?id=2",2222L),
                new Event("b","./ada?id=3",3212L));

        //按键进行分组,在进行聚合---->实现获取到当前用户最近一次访问时间
        /**
         * 方式1:匿名内部类的方式
         *
         * max : 只是将需要聚合的字段进行聚合操作,其他的字段还是使用第一次出现的值.
         * ------------
         * max> Event{user='a', url='./home', timestamp=1000}
         * max> Event{user='b', url='./h', timestamp=1212}
         * max> Event{user='c', url='./sdf', timestamp=1231}
         * max> Event{user='b', url='./h', timestamp=2002}
         * max> Event{user='c', url='./sdf', timestamp=3213}
         * max> Event{user='b', url='./h', timestamp=2222}
         * max> Event{user='b', url='./h', timestamp=3212}
         */
        SingleOutputStreamOperator<Event> maxTimestamp = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp"); //这里有两种方式,可以传入数组的下标,也可以使用指定的参数,由于这里是Pojo类型的数据,所以需指定.

        /**
         * 方式2: lambda
         *
         * maxBy: 的输入和输出相同,他只是做了一个聚合操作,他会将完整的数据输出.
         * ----------------
         * maxBy> Event{user='a', url='./home', timestamp=1000}
         * maxBy> Event{user='b', url='./h', timestamp=1212}
         * maxBy> Event{user='c', url='./sdf', timestamp=1231}
         * maxBy> Event{user='b', url='./ada?id=1', timestamp=2002}
         * maxBy> Event{user='c', url='./sdf', timestamp=3213}
         * maxBy> Event{user='b', url='./ada?id=2', timestamp=2222}
         * maxBy> Event{user='b', url='./ada?id=3', timestamp=3212}
         */
        stream.keyBy(data -> data.user)
                .maxBy("timestamp").print("maxby");
        //输出
        maxTimestamp.print("max");
        env.execute();

    }
}
