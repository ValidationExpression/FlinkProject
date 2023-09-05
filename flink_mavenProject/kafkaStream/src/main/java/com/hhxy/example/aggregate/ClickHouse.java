package com.hhxy.example.aggregate;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Random;

//SourceFunction<Event>这里的Event表示所返回的数据类型.
public class ClickHouse implements SourceFunction<Event> {
    //产生用户访问网页的模拟数据.
    //用户
    String[] user = {"liha","Tom","mary","小明","jary","maliao"};
    //网页
    String[] url = {"/a","/b","/c","/abc@a=1?","/al/a?","/b/url/afs"};
    //创建一个随机索引,将数据拼接为一个用户访问的网页数据
    Random random=new Random();
    //设置一个标记为,用于控制循环的开始和结束
    private boolean running=true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        //使用循环,模拟一直有用户访问网页
        while (running) {
            //主要就是实现SourceContext参数中的一个collect方法,收集数据并发送数据
            sourceContext.collect(new Event(user[random.nextInt(user.length)], url[random.nextInt(url.length)], System.currentTimeMillis()));
            //设置一个休息时间
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        //若想停止数据产生
        running=false;

    }
}
