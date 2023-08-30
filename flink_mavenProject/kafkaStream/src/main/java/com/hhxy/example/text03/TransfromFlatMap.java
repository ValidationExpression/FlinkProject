package com.hhxy.example.text03;

import com.hhxy.example.test02.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransfromFlatMap {
    public static void main(String[] args) throws Exception {
        // 1.创建连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.设置全局并行度
        env.setParallelism(1);
        // 3.数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<com.hhxy.example.test02.Event> stream = env.fromElements(new com.hhxy.example.test02.Event("a", "./home", 1000L),
                new com.hhxy.example.test02.Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L));

        // 4.转换算子flatMap
        /**
         * 1.自定义类继承flatMapFunction接口的方式
         */
        SingleOutputStreamOperator<String> flatMap = stream.flatMap(new MyFlatMap());
        /**
         * 2.匿名内部类的方式
         */
        /**
         * 3.lambda表达式
         */
        stream.flatMap((Event event,Collector<String> out) -> {
            //也可以进行条件过滤
            if (event.user.equals("b")){
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
            //也可以直接进行相应的数据拆分操作
            out.collect(event.user);
            out.collect(event.timestamp.toString());
            //他有可能出现flink的泛型擦除的现象,所以这里进行一个returns返回
        }).returns(new TypeHint<String>() {});
        //输出
        flatMap.print();
        env.execute();

    }

    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.timestamp.toString());        }
    }
}
