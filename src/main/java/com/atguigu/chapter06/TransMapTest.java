package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

//        SingleOutputStreamOperator<String> mapStream = stream.map(new MapFunction<Event, String>() {
//            @Override
//            public String map(Event event) throws Exception {
//                return event.name;
//            }
//        });
        SingleOutputStreamOperator<String> mapStream = stream.map(new UserExtractor());

        mapStream.print();

        env.execute();

    }

    public static class UserExtractor implements MapFunction<Event,String>{

        @Override
        public String map(Event event) throws Exception {

            return event.name;
        }
    }
}
