package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./home", 2000L)
        );

        SingleOutputStreamOperator<String> flatMapStream = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                if (event.name.equals("Maey")) {
                    System.out.print(event.name);
                } else if (event.name.equals("Bob")) {
                    System.out.print(event.name + event.url);
                }
            }
        });

        flatMapStream.print();

        env.execute();
    }
}
