package com.yyds.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWorkCount {
    //这里是批处理方式
    public static void main(String[] args) throws Exception {
        //1. 构建flink环境
        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据,这里的DataSource原码继承了DataSet所以是批处理方式
        DataSource<String> dataSource = ee.readTextFile("D:\\VMwareCentos\\flinkProject\\flink_mavenProject\\FlinkWorkCount\\input\\world.txt");
        //将每一行的数据进行分词,转换为二元组的形式,例如map ->(hello,1)只需要统计的个数
        FlatMapOperator<String, Tuple2<String, Integer>> wordTuple = dataSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            //分词得到每一行的词
            String[] words = line.split(" ");
            //遍历数组,将每一个单词进行转换为二元组输出
            for (String word : words) {
                //收集器,打散数据,多条输出
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));//由于java8的特性,所以这里需要进行数据类型转换

        //按照word进行分组,0表示的是(a1,a2)中的a1
        UnsortedGrouping<Tuple2<String, Integer>> wordGrouping = wordTuple.groupBy(0);

        //对分组进行统计,对第二个字段进行计数
        AggregateOperator<Tuple2<String, Integer>> sum =wordGrouping.sum(1);

        //输出
        sum.print();
    }
}
