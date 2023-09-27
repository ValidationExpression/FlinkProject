package com.hhxy.tableApi;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 实现: 使用sql语句处理数据流
 * 需求: 得到Event中的user,url
 */
public class TableAPITest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickHouse())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        //3.创建表结构执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将DataStream转换为Table, 这里的table就是转换后的表名
        Table table1 = tableEnv.fromDataStream(source);

        //5.使用sql语句进行相应的数据转换处理
        Table resultTable = tableEnv.sqlQuery("select user,url from" + table1);
        //6.输出,由于表数据无法直接输出,所以这里进行一次转换为DataStream数据流
        tableEnv.toDataStream(resultTable).print("result");

        env.execute();

    }
}
