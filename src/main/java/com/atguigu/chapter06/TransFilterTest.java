package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

//        SingleOutputStreamOperator<Event> filterStream = stream.filter(new FilterFunction<Event>() {
//            @Override
//            public boolean filter(Event event) throws Exception {
//                return event.name.equals("Mary");
//            }
//        });
        SingleOutputStreamOperator<Event> filterStream = stream.filter(new UserFilter());
        filterStream.print();
        env.execute();
    }

    public static class UserFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.name.equals("Mary");
        }
    }
}
