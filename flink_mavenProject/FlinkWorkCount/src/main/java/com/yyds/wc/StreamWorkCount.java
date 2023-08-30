package com.yyds.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWorkCount {
    public static void main(String[] args) throws Exception {
        //1.创建flink流的执行环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //---数据的来源可以为流式的方式,这样就会一直监听数据
        //读取文件,对比BatchWorkCount中的DataSource,这里的接收数据流
        DataStreamSource<String> dss = see.readTextFile("D:\\VMwareCentos\\flinkProject\\flink_mavenProject\\FlinkWorkCount\\input\\world.txt");

        //3.转换数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordTuple = dss.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            //分词得到每一行的词
            String[] words = line.split(" ");
            //遍历数组,将每一个单词进行转换为二元组输出
            for (String word : words) {
                //收集器,打散数据,多条输出
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));//由于java8的特性,所以这里需要进行数据类型转换

        //分组,使用lambda表达式获取到Tuple2<>元组的key,f0,f1
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordTuple.keyBy(data -> data.f0);
        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        //输出,
        sum.print();

        //需要监听一直执行,有数据来的时候,就执行一次
        see.execute();
    }
}
