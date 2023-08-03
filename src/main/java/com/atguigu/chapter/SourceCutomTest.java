package com.atguigu.chapter;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCutomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        //env.setParallelism(3);
        ClickSource clickSource = new ClickSource();
        //clickSource.cancel();
        DataStreamSource<Event> coustomStream = env.addSource(clickSource).setParallelism(2);

        coustomStream.print();

        env.execute();
    }
}
