package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );
        //将点击事件转换成长整型的时间戳输出
        SingleOutputStreamOperator<Long> RichStream = stream.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.print("索引为：" + getRuntimeContext().getIndexOfThisSubtask() + "的任务开始");
            }

            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.print("索引为：" + getRuntimeContext().getIndexOfThisSubtask() + "的任务结束");
            }
        });
        RichStream.print();

        env.execute();
    }
}
