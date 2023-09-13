package com.hhxy.example.product;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.security.acl.AclEntryImpl;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * 实现: 统计一个网页的访问数量,
 */
public class UrlCountTest {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        //2.数据源
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickHouse())
                //乱序流水位线,watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        source.print("data");

        //统计每个url访问量
        source.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //这里使用一个pojo类,来定义数据类型,分析为什么需要这种数据类型,
                .aggregate(new UrlViewCountAgg(),new UrlCountResult())
                .print();
        env.execute();

    }

    //自定义类实现aggregateFunction, 增量聚合用于计算访问的数量
    public static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L ;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong+1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    //窗口信息,这里可以获取到每一个窗口的信息,-->这里就会获得同一个窗口不同的url的数量信息,以此可以对url进行排序处理.
    public static class UrlCountResult extends ProcessWindowFunction<Long,UrlPojo,String, TimeWindow> {
        @Override   //这里的四个参数分别为输入,输出,key的类型, 时间窗口
        public void process(String url, ProcessWindowFunction<Long, UrlPojo, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlPojo> collector) throws Exception {
            Long start =context.window().getStart();
            Long end = context.window().getEnd();
            Long count =iterable.iterator().next();
            collector.collect(new UrlPojo(url,count,start,end));
        }
    }

    /**数据格式:
     * data> Event{user='maliao', url='/abc@a=1?', timestamp=2023-09-06 22:01:52}
     * data> Event{user='jary', url='/al/a?', timestamp=2023-09-06 22:01:53}
     * data> Event{user='Tom', url='/b/url/afs', timestamp=2023-09-06 22:01:54}
     * data> Event{user='Tom', url='/a', timestamp=2023-09-06 22:01:56}
     * data> Event{user='mary', url='/al/a?', timestamp=2023-09-06 22:01:57}
     * data> Event{user='liha', url='/abc@a=1?', timestamp=2023-09-06 22:01:58}
     * data> Event{user='maliao', url='/b/url/afs', timestamp=2023-09-06 22:01:59}
     * data> Event{user='maliao', url='/b', timestamp=2023-09-06 22:02:00}
     * -------------------------10s一个窗口--------------窗口时间含前不含后--------------
     * UrlPojo{url='/abc@a=1?', count=2, windowStart=2023-09-06 22:01:50.0, windowEnd=2023-09-06 22:02:00.0}
     * UrlPojo{url='/al/a?', count=2, windowStart=2023-09-06 22:01:50.0, windowEnd=2023-09-06 22:02:00.0}
     * UrlPojo{url='/a', count=1, windowStart=2023-09-06 22:01:50.0, windowEnd=2023-09-06 22:02:00.0}
     * UrlPojo{url='/b/url/afs', count=2, windowStart=2023-09-06 22:01:50.0, windowEnd=2023-09-06 22:02:00.0}
     */
}
