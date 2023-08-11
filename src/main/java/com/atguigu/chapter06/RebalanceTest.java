package com.atguigu.chapter06;

import com.atguigu.chapter.ClickSource;
import com.atguigu.chapter.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.rebalance().print("rebalance").setParallelism(4);

        env.execute();

    }
}
