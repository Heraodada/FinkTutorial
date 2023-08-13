package com.atguigu.chapter08;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
乱序流水线
 */
//        env.addSource(new ClickSource())
//                //插入水位线逻辑
//                .assignTimestampsAndWatermarks(
//                        //针对乱序流插入水位线，延迟时间设置为5s
//                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<Event>() {
//                                            //抽取时间线规则
//                                            @Override
//                                            public long extractTimestamp(Event event, long l) {
//                                                return event.timestamp;
//                                            }
//                                        }
//                                )
//                ).print();

        /*
        有序流水线
         */
            env.addSource(new ClickSource())
                    //插入水位线逻辑
                            .assignTimestampsAndWatermarks(
                                    //针对有序流插入水位线
                                    WatermarkStrategy.<Event>forMonotonousTimestamps()
                                            .withTimestampAssigner(
                                                    new SerializableTimestampAssigner<Event>() {
                                                        //抽取时间线规则
                                                        @Override
                                                        public long extractTimestamp(Event event, long l) {
                                                            return event.timestamp;
                                                        }
                                                    }
                                            )
                            ).print();

        env.execute();
    }
}
