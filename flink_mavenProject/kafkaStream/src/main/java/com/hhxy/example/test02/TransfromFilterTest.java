package com.hhxy.example.test02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromFilterTest {
    public static void main(String[] args) throws Exception {
        // 1.创建连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.设置全局并行度
        env.setParallelism(1);
        // 3.数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L));

        // 4.转换算子filter(FilterFunction<T>)
        /**
         * 实现方法1: 自定义类继承接口
         */
        SingleOutputStreamOperator<Event> filter = stream.filter(new MyFilter("b"));
        /**
         * 实现方式2: 匿名内部类的方式
         */
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("b");
            }
        });
        /**
         * 实现方式3: lambda表达式
         */
        stream.filter(data -> data.user.equals("b"));


        // 5.输出
        filter.print();
        env.execute();
    }

    //自定义filter
    public static class MyFilter implements FilterFunction<Event>{
        private String key;
        //在定义一个构造方法,用于将参数传入
        public MyFilter(String key){
            this.key=key;
        }
        @Override
        public boolean filter(Event event) throws Exception {
            //这里可以进行是否达到所需要的过滤条件
            return event.user.equals(this.key); //只会输出用户为b 的数据,其他的数据将会过滤
            //这里可以更加灵活,使用变量限定,定义一个公共变量
        }
    }
}
