package com.hhxy.example08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 需求:实现两条流的合并, 使用Connection的方式
 */
public class ConnectionTest {
    public static void main(String[] args) throws Exception {
        //1.创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源,
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> source2 = env.fromElements(1L, 2L, 3L);

        SingleOutputStreamOperator<String> map = source1.connect(source2)
                .map(new CoMapFunction<Integer, Long, String>() {
                    //这里就是将两条流和合并具体方式
                    @Override
                    public String map1(Integer integer) throws Exception {
                        return "int:" + integer.toString();
                    }

                    @Override
                    public String map2(Long aLong) throws Exception {
                        return "Long:" + aLong.toString();
                    }
                });

        //3.输出,这里将都以String的方式进行输出
        map.print();
        env.execute();

    }
}
