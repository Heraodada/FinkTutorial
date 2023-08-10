package com.atguigu.chapter06;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //匿名函数方法实现
//        SingleOutputStreamOperator<Event> filterStream = stream.filter(new FilterFunction<Event>() {
//            @Override
//            public boolean filter(Event event) throws Exception {
//                return event.name.contains("Mary");
//            }
//        });

        //类名实现方式
        SingleOutputStreamOperator<Event> filterStream = stream.filter(new KeyWordFilter("home"));
        filterStream.print();

        env.execute();
    }
    public static class KeyWordFilter implements FilterFunction<Event>{
        private String keyWord;

        public KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains(this.keyWord);
        }
    }
}
