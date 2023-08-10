package com.atguigu.chapter06;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFunctionLambdatest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //map匿名函数
//        SingleOutputStreamOperator<String> stream1 = stream.map(event -> event.url);
//        stream1.print();

        //flatmap匿名函数
//        SingleOutputStreamOperator<Object> stream2 = stream.flatMap((event, out) -> {
//            out.collect(event.url);
//        });
//        stream2.print();
        // flatMap 使用 Lambda 表达式，必须通过 returns 明确声明返回类型
//        SingleOutputStreamOperator<String> stream3 = stream.flatMap((Event event, Collector<String> out) -> {
//            out.collect(event.url);
//        }).returns(Types.STRING);
//        stream3.print();

        stream.map(event -> Tuple2.of(event.name,1L)).returns(Types.TUPLE(Types.STRING,Types.LONG)).print();
        env.execute();
    }
}
