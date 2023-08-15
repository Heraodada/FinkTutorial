package com.atguigu.chapter08;

import com.atguigu.chapter.ClickSource;

import com.atguigu.chapter.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.DecimalFormat;
import java.util.HashSet;

public class WindowAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
        );
        // 所有数据设置相同的 key，发送到同一个分区统计 PV 和 UV，再相除
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                .aggregate(new AvgPv()).print();

        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>,Long>,Double>{

        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            //创建累加器
            return Tuple2.of(new HashSet<String>(),0l);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
            hashSetLongTuple2.f0.add(event.user);
            return Tuple2.of(hashSetLongTuple2.f0, hashSetLongTuple2.f1+1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
            double round = (double) (Math.round((double) hashSetLongTuple2.f1 / hashSetLongTuple2.f0.size()));

            return round;
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
            return null;
        }
    }
}
