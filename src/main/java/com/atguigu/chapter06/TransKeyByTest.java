package com.atguigu.chapter06;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./home", 2000L),
                new Event("Bob", "./home", 1000L)

        );

        //Lambda表达式
        //KeyedStream<Event, String> KeyByStream = stream.keyBy(e -> e.name);

        //使用匿名类实现 KeySelector
        KeyedStream<Event, String> KeyByStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.name;
            }
        });

        KeyByStream.print();
        env.execute();
    }
}
