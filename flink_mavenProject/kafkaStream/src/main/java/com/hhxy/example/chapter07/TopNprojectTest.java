package com.hhxy.example.chapter07;

import com.hhxy.example.product.UrlPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 需求: 求一个网站的不同网页的访问量的排行.
 * 条件: 统计每个10s内的网页访问量的排行, 每5s刷新一次.
 * --这里可以得到一个信息,就是有一个10s的滑动窗口,滑动的时间为5s.
 */
public class TopNprojectTest {
    public static void main(String[] args) throws Exception {
        //1.创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.数据源
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickHouse())
                //时间线,这里的顺序是乱序, 但是0延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        source.print("date");

        //3.数据转换,按照url进行分类
        SingleOutputStreamOperator<UrlPojo> aggregate = source.keyBy(data -> data.url)
                //这里需要进行开窗,滑动窗口窗口时间10s, 滑动时间5s
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountAgg1(), new UrlCountResult1());

        aggregate.print("-->");

        //使用keyedProcesstionFunction()处理函数,
        aggregate.keyBy(data -> data.windowEnd)
                .process(new TopNProcessFunction(2))
                .print();

        env.execute();

    }
    //自定义类实现aggregateFunction, 增量聚合用于计算访问的数量
    public static class UrlViewCountAgg1 implements AggregateFunction<Event,Long,Long> {
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
    public static class UrlCountResult1 extends ProcessWindowFunction<Long, UrlPojo,String, TimeWindow> {
        @Override   //这里的四个参数分别为输入,输出,key的类型, 时间窗口
        public void process(String url, ProcessWindowFunction<Long, UrlPojo, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlPojo> collector) throws Exception {
            Long start =context.window().getStart();
            Long end = context.window().getEnd();
            Long count =iterable.iterator().next();
            collector.collect(new UrlPojo(url,count,start,end));
        }
    }


    /**
     * 这里自定义类继承keyedProcessionFunction
     */
    public static class TopNProcessFunction extends KeyedProcessFunction<Long,UrlPojo,String>{
        //定义要给属性
        private Integer n;

        //定义列表状态
        private ListState<UrlPojo> urlPojoListState;
        public TopNProcessFunction(Integer n) {
            this.n=n;
        }

        //注意由于flink运行的是流动的数据,所以不能将列表状态设置为实例化的方式实现
        //应当使用此时的环境状态


        @Override
        public void open(Configuration parameters) throws Exception {
            //固定用法
            urlPojoListState=getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlPojo>("url-count-list", Types.POJO(UrlPojo.class))
            );
        }

        @Override
        public void processElement(UrlPojo urlPojo, KeyedProcessFunction<Long, UrlPojo, String>.Context context, Collector<String> collector) throws Exception {
            //将数据保存到状态中
            urlPojoListState.add(urlPojo);
            //注册定时器timeEnd+1ms                    context.getCurrentKey()获取当前的key,由于当前的key是timeEnd所以这里是时间.
            context.timerService().registerEventTimeTimer(context.getCurrentKey()+1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlPojo, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定义一个Pojo类型的数组,用于存储,一个窗口的数据
            ArrayList<UrlPojo> urlPojos=new ArrayList<>();
            //遍历将列表状态中的数据放入到urlPojos数组中
            for (UrlPojo urlPojo: urlPojoListState.get()){
                urlPojos.add(urlPojo);
            }

            //然后进行相应的排序操作
            urlPojos.sort(new Comparator<UrlPojo>() {
                @Override
                public int compare(UrlPojo o1, UrlPojo o2) {
                    //o2-o1降序排序, 这里还需要进行数值类型转换
                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            //输出信息
            StringBuilder topResult =new StringBuilder();
            //取出数组中的前两个
            for (int i = 0; i < n; i++) {
                UrlPojo pojo = urlPojos.get(i);
                String info = "No."+(i+1)+" "
                        +"url:"+ pojo.url + " "
                        +"访问量:"+ pojo.count +"\n";
                topResult.append(info);
            }
            topResult.append("----------------------");
            out.collect(topResult.toString());
        }
    }

}
/**
 * 这里的数据是10s一个窗口,5s进行一次刷新.
 * //注意这里的数据是经过统计的数据,不是一次访问页面的数据,所以也就可能出现,开始没有的数据,一出来就是3等其他数值.
 * ------------>这里可以在重新运行得出结果<--------------------
 * -->> UrlPojo{url='/b/url/afs', count=1, windowStart=2023-09-13 21:29:45.0, windowEnd=2023-09-13 21:29:55.0}
 * -->> UrlPojo{url='/c', count=1, windowStart=2023-09-13 21:29:45.0, windowEnd=2023-09-13 21:29:55.0}
 * -->> UrlPojo{url='/b', count=1, windowStart=2023-09-13 21:29:45.0, windowEnd=2023-09-13 21:29:55.0}
 * No.1 url:/b/url/afs 访问量:1
 * No.2 url:/c 访问量:1
 * ----------------------
 * -->> UrlPojo{url='/al/a?', count=1, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * -->> UrlPojo{url='/b/url/afs', count=1, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * -->> UrlPojo{url='/b', count=2, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * -->> UrlPojo{url='/abc@a=1?', count=1, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * -->> UrlPojo{url='/c', count=2, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * -->> UrlPojo{url='/a', count=1, windowStart=2023-09-13 21:29:50.0, windowEnd=2023-09-13 21:30:00.0}
 * //b的次数是2的解释,由于它是10s的窗口,所以看第一次开始的时间21:29:45.0 ,
 * 然后过了5s进行了一次刷新,此时第一次10s的窗口还开着,所以此时b的访问次数+1 =2
 * No.1 url:/b 访问量:2
 * No.2 url:/c 访问量:2
 * ----------------------
 * -->> UrlPojo{url='/abc@a=1?', count=2, windowStart=2023-09-13 21:29:55.0, windowEnd=2023-09-13 21:30:05.0}
 * -->> UrlPojo{url='/al/a?', count=2, windowStart=2023-09-13 21:29:55.0, windowEnd=2023-09-13 21:30:05.0}
 * -->> UrlPojo{url='/b', count=2, windowStart=2023-09-13 21:29:55.0, windowEnd=2023-09-13 21:30:05.0}
 * -->> UrlPojo{url='/c', count=2, windowStart=2023-09-13 21:29:55.0, windowEnd=2023-09-13 21:30:05.0}
 * -->> UrlPojo{url='/a', count=2, windowStart=2023-09-13 21:29:55.0, windowEnd=2023-09-13 21:30:05.0}
 * //到了第2次刷新,已经过了10s,且窗口的时间范围是含前不含后,所以此时b的数量还是 2
 * No.1 url:/abc@a=1? 访问量:2
 * No.2 url:/al/a? 访问量:2
 * ----------------------
 * -->> UrlPojo{url='/c', count=1, windowStart=2023-09-13 21:30:00.0, windowEnd=2023-09-13 21:30:10.0}
 * -->> UrlPojo{url='/abc@a=1?', count=3, windowStart=2023-09-13 21:30:00.0, windowEnd=2023-09-13 21:30:10.0}
 * -->> UrlPojo{url='/al/a?', count=3, windowStart=2023-09-13 21:30:00.0, windowEnd=2023-09-13 21:30:10.0}
 * -->> UrlPojo{url='/b', count=1, windowStart=2023-09-13 21:30:00.0, windowEnd=2023-09-13 21:30:10.0}
 * -->> UrlPojo{url='/a', count=2, windowStart=2023-09-13 21:30:00.0, windowEnd=2023-09-13 21:30:10.0}
 * No.1 url:/abc@a=1? 访问量:3
 * No.2 url:/al/a? 访问量:3
 */
