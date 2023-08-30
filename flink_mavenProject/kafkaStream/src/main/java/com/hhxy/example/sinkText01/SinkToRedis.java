package com.hhxy.example.sinkText01;

import com.hhxy.example.test02.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.JedisPoolConfig;

public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        //1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //2.数据源
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L),
                new Event("b", "./ada?id=1", 2002L),
                new Event("c", "./sdf", 3213L),
                new Event("b", "./ada?id=2", 2222L),
                new Event("b", "./ada?id=3", 3212L));

        //RedisSink<>(参数1,参数2),参数1:jedis的连接配置,参数2:需要传入的数据.
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")  //地址
                .build();
        //3.输出位置redis
        stream.addSink(new RedisSink<>(config,new MyRedisMapper()));

        env.execute();
    }

    //自定义一个类用于继承RedisMapper接口以实现相应的数据传入.
    public static class MyRedisMapper implements RedisMapper<Event>{
        //作用用于返回redis操作的一个描述
        @Override
        public RedisCommandDescription getCommandDescription() {
            //RedisCommand.HSET表示相redis写入一个hash表,  "clicks": hash表的key
            return new RedisCommandDescription(RedisCommand.HSET,"clicks");
        }

        @Override
        public String getKeyFromData(Event event) {  //key的值
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {  //value
            return event.url;
        }
    }
}
