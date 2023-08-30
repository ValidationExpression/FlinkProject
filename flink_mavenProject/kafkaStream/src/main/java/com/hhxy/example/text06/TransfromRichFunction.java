package com.hhxy.example.text06;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromRichFunction {
    public static void main(String[] args) throws Exception {
        // 1.创建连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.设置全局并行度
        env.setParallelism(1);
        // 3.数据源-->java对象Event类型的数据(这里的数据由于只是进行测试,所以写死数据)
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L));

        //
        stream.map(new MyRichMapper()).print();
        env.execute();
    }

    //实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用"+getRuntimeContext().getIndexOfThisSubtask()+"号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.user.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用"+getRuntimeContext().getIndexOfThisSubtask()+"号任务结束");
        }

    }
}
