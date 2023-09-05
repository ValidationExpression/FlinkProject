package com.hhxy.example.sinkText01;

import com.hhxy.example.test02.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkToES {
    public static void main(String[] args) throws Exception {
        //1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //2.添加数据
        DataStreamSource<Event> stream = env.fromElements(new Event("a", "./home", 1000L),
                new Event("b", "./h", 1212L),
                new Event("c", "./sdf", 1231L),
                new Event("b", "./ada?id=1", 2002L),
                new Event("c", "./sdf", 3213L),
                new Event("b", "./ada?id=2", 2222L),
                new Event("b", "./ada?id=3", 3212L));

        //3.
        //定义hosts列表
        ArrayList<HttpHost> httpHosts=new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9870));

        //定义esSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            //这个方法就是数据的具体流出方式.
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                //构建IndexRequest
                IndexRequest request = Requests.indexRequest()
                        .index("click")
                        .source(map);
                requestIndexer.add(request);
            }
        };

        stream.addSink(new ElasticsearchSink.Builder<>(httpHosts,elasticsearchSinkFunction).build());

        env.execute();

    }
}
