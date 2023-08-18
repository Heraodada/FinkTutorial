package com.atguigu.chapter08;

import com.atguigu.chapter.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.time.Duration;
import java.util.stream.StreamSupport;

public class WatermarkTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //将数据源改为socket文本流，并转换伟Event类型
        env.socketTextStream("172.30.6.152",7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()));
                    }
                })
                //插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        //针对乱序流插入水位线，延迟时间设置为5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )
        //根据user分组，开窗统计
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestResult())
                .print();

        env.execute();
    }

    public static class WatermarkTestResult extends ProcessWindowFunction<Event,String,String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, java.lang.Iterable<Event> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            //获取当前的水位线
            long currentWatermark = context.currentWatermark();
            //获取迭代器集合元素数据
            Long count = iterable.spliterator().getExactSizeIfKnown();
            collector.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素， 窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
