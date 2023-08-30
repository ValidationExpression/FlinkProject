package com.hhxy.example.text01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.spi.LoggerRegistry;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        // 1.创建连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.设置并行度
        env.setParallelism(1);
        // 3.数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L));

        // 4.转换算子计算-->可以按照你所需要的方式进行相应的转换-(这里是提取了用户)
        /**
         *  1. map(MapFunction<T,O>) ---->使用自定义的类继承接口
         */
        SingleOutputStreamOperator<String> map = stream.map(new MyMapper());
        /**
         * 2.使用匿名类实现接口,这样实现的方式更为简单.
         */
        SingleOutputStreamOperator<String> map1 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        /**
         * 3.使用java8的lambda表达式的方式
         * 由于flink存在泛型擦除的现象,所以在使用lambda可能不知道数据类型,所以在最后可以写上一个return返回数据类型.
         */
        SingleOutputStreamOperator<String> map2 = stream.map(data -> data.user);
        // 5.数据输出
        map.print();
        env.execute();

    }

    //定义一个类进行实现MapFunction接口,是一种基础的方式,(可以使用匿名内部类的方式进行)
    //自定义mapFunction,MapFunction<T,O>其中的T表示的是输入的数据类型,O表示的是输出的数据类型
    public static class MyMapper implements MapFunction<Event, String>{ //输入Event,输出String
        @Override
        public String map(Event event) throws Exception {
            //这里也可以进行一系列的不同转换,最终的到所需要的数据格式
            return event.user;  //这里的需求是的到用户user
        }
    }
}
